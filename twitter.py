# Code to download the timeline of a list of Twitter users. It authenticates at
#   Twitter through application-only authentication [1]. All the tweets and
#   metadata is saved in JSON format in a directory for each user.
#
# References:
#   [1] https://developer.twitter.com/en/docs/basics/authentication/overview/application-only
#   [2] https://developer.twitter.com/en/docs/basics/authentication/api-reference/token
#   [3] https://developer.twitter.com/en/docs/basics/rate-limiting
#   [4] https://developer.twitter.com/en/docs/basics/rate-limits
#   [5] https://developer.twitter.com/en/docs/developer-utilities/rate-limit-status/api-reference/get-application-rate_limit_status
#   [6] https://developer.twitter.com/en/docs/basics/response-codes
#   [7] https://developer.twitter.com/en/docs/tweets/search/overview
#   [8] https://developer.twitter.com/en/docs/tweets/search/guides/build-standard-query
#   [9] https://developer.twitter.com/en/docs/tweets/search/guides/standard-operators
#   [10] https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
#   [11] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json
#   [12] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
#   [13] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
#   [14] https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-show
#   [15] https://developer.twitter.com/en/docs/tweets/timelines/guides/working-with-timelines
#   [16] https://developer.twitter.com/en/docs/tweets/timelines/api-reference/get-statuses-user_timeline


import logging
import sys
import http
import http.client
import traceback
import base64
import urllib
import json
import time


class TwitterUserNotFoundException(Exception):
    pass


class TwitterUserSuspendedException(Exception):
    pass


class ProtectedTweetsException(Exception):
    pass


class TwitterServerErrorException(Exception):
    pass


class TwitterReader:


    ##### PRIVATE CLASS MEMBERS #####


    _endpoint                       = 'api.twitter.com'
    _connection                     = None
    _debug_connection               = None
    _request_headers                = None

    _app_name                       = None
    _consumer_key                   = None
    _consumer_secret                = None

    _limits = {                                                             # dictionary containing resources rate limits information
                '/search/tweets'             : {                            # resource
                                                'remaining'     : None,     # how many requests are left for the resource
                                                'renew_epoch'   : None,     # next epoch to renew the window for the resource
                                  },
                '/users/show'                : {                            # resource
                                                'remaining'     : None,     # how many requests are left for the resource
                                                'renew_epoch'   : None,     # next epoch to renew the window for the resource
                                  },
                '/statuses/user_timeline'    : {                            # resource
                                                'remaining'     : None,     # how many requests are left for the resource
                                                'renew_epoch'   : None,     # next epoch to renew the window for the resource
                                              },
              }

    _logger                         = None


    def __init__(self, app_name, consumer_key, consumer_secret, debug_connection = False):
        self._app_name = app_name
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._debug_connection = debug_connection

        self._limits['/search/tweets']['remaining'] = 1          # value of one to allow the first request, after then the value is updated from Twitter headers
        self._limits['/users/show']['remaining'] = 1             # value of one to allow the first request, after then the value is updated from Twitter headers
        self._limits['/statuses/user_timeline']['remaining'] = 1 # value of one to allow the first request, after then the value is updated from Twitter headers

        self._logger = logging.getLogger(self.__class__.__name__)


    def _handle_twitter_response_code(self, response, data, user_id = ''):
        if response.status == http.HTTPStatus.OK:
            return
        try:
            twitter_error = json.loads(data, encoding='utf-8')['errors'][0]
            twitter_error_msg = ''.join(['Twitter error message: ', str(twitter_error['code']), ' - ', twitter_error['message']])
        except Exception as e:
            twitter_error_msg = '(empty or invalid Twitter error message)'
        error_msg = ''.join(['HTTP error message: ', str(response.status), ' - ', response.reason, '. ', twitter_error_msg])
        if response.status == http.HTTPStatus.NOT_FOUND:            # inexistent user, maybe has got himself out from Twitter
            raise TwitterUserNotFoundException(''.join(['User id = ', user_id, ' not found. ', error_msg]))
        elif response.status == http.HTTPStatus.FORBIDDEN:          # suspended user
            raise TwitterUserSuspendedException(''.join(['User id = ', user_id, ' suspended. ', error_msg]))
        elif response.status == http.HTTPStatus.UNAUTHORIZED:       # protected tweet
            raise ProtectedTweetsException(''.join(['Tweets from user id = ', user_id, ' are protected. ', error_msg]))
        elif (response.status // 100) == 5 :                        # HTTP server error
            raise TwitterServerErrorException('HTTP server error. ' + error_msg)
        else:
            raise Exception(error_msg)


    def _get_request_headers(self):
        consumer_cred_base64 = base64.b64encode(bytes(self._consumer_key + ':' + self._consumer_secret, 'ascii'))
        bearer_token_headers = {'Host': self._endpoint,
                                'User-Agent': self._app_name,
                                'Authorization': 'Basic ' + str(consumer_cred_base64, 'ascii'),
                                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                                #'Accept-Encoding': 'gzip',     # gives error
                               }
        bearer_token_params = urllib.parse.urlencode({'grant_type': 'client_credentials'})
        self._connection.request('POST', '/oauth2/token', headers=bearer_token_headers, body=bearer_token_params)
        response = self._connection.getresponse()
        data = response.read().decode('utf-8')  # See note on https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
        self._handle_twitter_response_code(response, data)
        bearer_token_dict = json.loads(data, encoding='utf-8')
        if ('token_type' not in bearer_token_dict) or (bearer_token_dict['token_type'] != 'bearer'):
            raise Exception(''.join(['Invalid JSON response from Twitter : ', str(bearer_token_dict)]))
        self._request_headers = { 'Host': self._endpoint ,
                                  'User-Agent': self._app_name,
                                  'Authorization': 'Bearer ' + bearer_token_dict['access_token'],
                                  'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                                  #'Accept-Encoding': 'gzip',     # gives error
                                }


    def _get_rate_limit_status(self, resource):
        family = resource.split('/')[1]
        params = { 'resources' : family }
        encoded_params = '?%s' % urllib.parse.urlencode(params)
        retry = True
        while retry:
            self._logger.debug(''.join(['Absent rate limit headers. Requesting rate limits for resource family ', family , ' ...']))
            self._connection.request('GET', '/1.1/application/rate_limit_status.json' + encoded_params, headers=self._request_headers)
            response = self._connection.getresponse()
            data = response.read().decode('utf-8')  # See note on https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
            response_key = resource + '/:id' if resource == '/users/show' else resource
            try:
                self._handle_twitter_response_code(response, data)
                limits = json.loads(data, encoding='utf-8')
                self._limits[resource]['remaining'] = limits['resources'][family][response_key]['remaining']
                self._limits[resource]['renew_epoch'] = limits['resources'][family][response_key]['reset']
                retry = False
            except Exception as e:
                self._logger.warning(''.join(['Error requesting rate limits for resource family ', family , ' . Error: ', str(e), ' Sleeping 5 seconds and retrying ...']))
                time.sleep(5)       # this 'application/rate_limit_status' resource can be queried 180 times in a 15-minutes window (at each 5 seconds)


    def _check_limit_remaining(self, resource):
        if self._limits[resource]['remaining'] <= 0:
            if self._limits[resource]['remaining'] == -1:       # Twitter didn't send the rate limits headers, request these limits
                self._get_rate_limit_status(resource)
            if self._limits[resource]['remaining'] == 0:
                sleep_sec = (self._limits[resource]['renew_epoch'] + 1) - time.time() # (renew_epoch + 1) => avoiding synchonization problems
                sleep_sec = 0 if sleep_sec < 0 else sleep_sec
                self._logger.debug(''.join(['Requests limit reached. Sleeping for ', str(sleep_sec), ' seconds ...']))
                time.sleep(sleep_sec)
                self.reconnect()    # better to force a restart since the server maybe had dropped the current connection


    def _update_rate_limit(self, resource, response):
        self._limits[resource]['remaining'] = int(response.getheader('x-rate-limit-remaining', default='-1')) # header can be absent
        self._limits[resource]['renew_epoch'] = int(response.getheader('x-rate-limit-reset', default='-1'))   # header can be absent


    def _request_tweets(self, params):
        self._check_limit_remaining('/statuses/user_timeline')
        encoded_params = '?%s' % urllib.parse.urlencode(params)
        self._connection.request('GET', '/1.1/statuses/user_timeline.json' + encoded_params, headers=self._request_headers)
        response = self._connection.getresponse()
        data = response.read().decode('utf-8')  # See note on https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
        self._handle_twitter_response_code(response, data, params['user_id'])
        self._update_rate_limit('/statuses/user_timeline', response)
        return json.loads(data, encoding='utf-8')


    ##### PUBLIC CLASS MEMBERS #####


    def connect(self):
        self._logger.debug(''.join(['Connecting to Twitter endpoint ', self._endpoint, ' ...']))
        self._connection = http.client.HTTPSConnection(self._endpoint)
        self._connection.set_debuglevel(1 if self._debug_connection else 0)
        if not self._request_headers:
            self._logger.debug('Trying to get application bearer token ...')
            self._get_request_headers()


    def cleanup(self):
        self._connection.close()


    def reconnect(self):
        self._logger.info(''.join(['Restarting connection to Twitter endpoint ', self._endpoint, ' ...']))
        try:
            self._connection.close()
        except Exception as e:
            self._logger.warning('Error trying to close twitter connection while reconnecting.')
            traceback.print_exc()
        self.connect()


# STOPPED
# Besides using the function words to filter the language of the user, the code
#   also uses the parameter 'language' in the Twitter search API. The query
#   option '-filter:retweets' is also used in the search API to filter out
#   retweets. It is recommended not to use too small function words (less than 3
#   characters) since they can match undesired languages.
#
# Pseudo-code:
#   read list of words
#   for each 'word':
#       search most recents tweets using 'word' and recover at most max_results_per_word parameter
#       for each 'tweet':
#           save user information if not already seen
    def search_users(self, word, language, max_results):
        search_params = {'q':                   word + ' -filter:retweets',
                         'lang' :               language,
                         'result_type' :        'recent',
                         'count' :              100,
                         'include_entities' :   'true',
                 }
        encoded_search_params = '?%s' % urllib.parse.urlencode(search_params)
        acc_results = 0
        users = {}
        while acc_results < max_results:
            # get tweets
            self._check_limit_remaining('/search/tweets')
            self._connection.request('GET', '/1.1/search/tweets.json' + encoded_search_params, headers=self._request_headers)
            response = self._connection.getresponse()
            data = response.read().decode('utf-8')  # See note on https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
            self._handle_twitter_response_code(response, data)
            tweets = json.loads(data, encoding='utf-8')
            self._update_rate_limit('/search/tweets', response)

            # find users
            for tweet in tweets['statuses']:
                if tweet['user']['id_str'] in users:
                    self._logger.debug(''.join([ '\t\tUser ', tweet['user']['screen_name'], ' , id = ', tweet['user']['id_str'], ' already seen. Ignoring ...' ]))
                    continue
                self._logger.debug(''.join([ '\t\tAdding user ', tweet['user']['screen_name'], ' , id = ', tweet['user']['id_str'] ]))
                users[tweet['user']['id_str']] = tweet['user']

            # account results
            results = len(tweets['statuses'])
            acc_results += results
            self._logger.debug(''.join(['\tRetrieved ', str(results), ' tweets. Current Number of users found = ',  str(len(users.keys())), '. Remaining \'/search/tweets requests\' = ', str(self._limits['/search/tweets']['remaining']), '.']))

            # get next results page
            if 'next_results' not in tweets['search_metadata']:     # end of results
                break
            encoded_search_params = tweets['search_metadata']['next_results']

        self._logger.debug(''.join(['Number of users found for word ', word, ' = ',  str(len(users.keys())), '.']))
        return users


    def get_user_info(self, user_id):
        self._check_limit_remaining('/users/show')
        self._connection.request('GET', ''.join(['/1.1/users/show.json?user_id=', user_id]), headers=self._request_headers)
        response = self._connection.getresponse()
        data = response.read().decode('utf-8')  # See note on https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
        self._handle_twitter_response_code(response, data, user_id)
        self._update_rate_limit('/users/show', response)
        self._logger.debug(''.join(['Remaining \'/users/show\' requests = ', str(self._limits['/users/show']['remaining']), '.']))
        return json.loads(data, encoding='utf-8')


    #   Download all the tweets in the user timeline according to [13]
    def get_user_timeline(self, user_id):
        timeline_url = '/1.1/statuses/user_timeline.json'
        timeline_params = {'user_id'            : user_id,
                           'count'              : 200,
                           'include_rts'        : 'true',
                           'exclude_replies'    : 'false',
                           'trim_user'          : 'true',
                          }

        # first timeline request
        tweets = self._request_tweets(timeline_params)
        retrieved_tweets = len(tweets)
        self._logger.debug(''.join(['Retrieved ', str(retrieved_tweets), ' tweets in the first request. Remaining \'/statuses/user_timeline\' requests = ', str(self._limits['/statuses/user_timeline']['remaining']), '.']))
        if retrieved_tweets == 0:    # finish this profile collecting
            return tweets

        # older tweets
        while retrieved_tweets > 0:
            timeline_params['max_id'] = tweets[-1]['id'] - 1
            temp = self._request_tweets(timeline_params)
            retrieved_tweets = len(temp)
            self._logger.debug(''.join(['Retrieved ', str(retrieved_tweets), ' tweets. Remaining \'/statuses/user_timeline\' requests = ', str(self._limits['/statuses/user_timeline']['remaining']), '.']))
            tweets += temp
        del timeline_params['max_id']

        # newer tweets since collecting
        timeline_params['since_id'] = tweets[0]['id']
        temp = self._request_tweets(timeline_params)
        self._logger.debug(''.join(['Retrieved ', str(len(temp)), ' newer tweets since collecting. Remaining \'/statuses/user_timeline\' requests = ', str(self._limits['/statuses/user_timeline']['remaining']), '.']))
        return temp + tweets
