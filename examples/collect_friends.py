#!/usr/bin/env python3


import sys
sys.path.append('..')
import argparse
import logging
import os
import twitter
import time
import json
import traceback


def command_line_parsing():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user-id', '-u',
                        dest='user_id',
                        required=True,
                        help='Username ID that will have its social data analyzed.')
    parser.add_argument('--dest-dir', '-e',
                        dest='dest_dir',
                        required=True,
                        help='Directory where the output files will be written.')
    parser.add_argument('--high-connection-threshold', '-c',
                        dest='high_connection_threshold',
                        type=int,
                        default=200,
                        help='Maximum threshold for number of followers and friends a friend must have not to be excluded from the analysis (since it is considered as a celebrity or a bot). For no limiting, use 0 for this value. Default = 200.')
    parser.add_argument('--debug', '-d',
                        dest='debug',
                        type=int,
                        choices = [0, 1, 2],
                        nargs='?',
                        const=1,
                        default=0,
                        help='Print debug information. 0 = no debug (default); 1 = normal debug; 2 = deeper debug (HTTP debug).')
    return parser.parse_args()


def add_twitter_screen_name(twitter_conn, tweets):
    user_ids = set()
    for tweet in tweets:
        for retweeter in tweet['retweeters']:
            user_ids.add(retweeter)
    translation_table = {}
    for user_id in user_ids:
        translation_table[user_id] = twitter_conn.get_user_info(str(user_id))['screen_name']
    for tweet in tweets:
        new_retweeters = []
        for retweeter in tweet['retweeters']:
            new_retweeters.append({'id': retweeter, 'screen_name': translation_table[retweeter]})
        tweet['retweeters'] = new_retweeters
    return tweets


def filter_twitter_high_connected(users, threshold):
    new_list = []
    for user in users:
        if user['followers_count'] <= threshold and user['friends_count'] <= threshold:
            new_list.append(user)
        else:
            logging.debug('\t\tRemoving user {} from list. Followers count: {}, friends count: {}.'.format(user['screen_name'], user['followers_count'], user['friends_count']))
    return new_list


def retrieve_twitter_data(twitter_userid, dest_dir, connectivity_threshold):
    logging.info('\tConnecting to Twitter ...')
    app_name        = '<your application name>'
    consumer_key    = '<your application consumer key>'
    consumer_secret = '<your application consumer secret>'
    twitter_conn = twitter.TwitterReader(app_name,
                                         consumer_key,
                                         consumer_secret,
                                         debug_connection = (args.debug == 2)
                                        )
    twitter_conn.connect()

    logging.info('\tRetrieving user information and retweeters ...')
    retry = True
    while retry:
        logging.debug(''.join(['\t\tRetrieving user information and timeline from user ', twitter_userid, ' ...']))
        try:
            user_info = twitter_conn.get_user_info(twitter_userid)
            user = {'id'            :user_info['id'],
                    'screen_name'   :user_info['screen_name'],
                   }
            with open(os.sep.join([dest_dir, 'user.json']), mode='wt', encoding='ascii') as fd:
                json.dump(user, fd, indent=4, sort_keys=True)
            tweets = twitter_conn.get_user_timeline(twitter_userid)
            tweets_retweets = []
            for tweet in tweets:
                element = {}
                element['text'] = tweet['text']
                element['id'] = tweet['id']
                element['retweeters'] = twitter_conn.get_retweeters(tweet['id'])
                tweets_retweets.append(element)
            logging.debug('\t\tRecovering screen_name attributes ...')
            tweets_retweets = add_twitter_screen_name(twitter_conn, tweets_retweets)
            with open(os.sep.join([dest_dir, 'tweets.json']), mode='wt', encoding='ascii') as fd:
                json.dump(tweets_retweets, fd, indent=4, sort_keys=True)
        except twitter.TwitterUserNotFoundException as tunfe:
            logging.warning(''.join(['\t\t', str(tunfe), ' Aborting user timeline ...']))
        except twitter.TwitterUserSuspendedException as tuse:
            logging.warning(''.join(['\t\t', str(tuse), ' Aborting user timeline ...']))
        except twitter.ProtectedTweetsException as pte:
            logging.warning(''.join(['\t\t', str(pte), ' Aborting user timeline ...']))
        except twitter.TwitterServerErrorException as tsee:
            retry_sleep_sec = 60
            logging.warning(''.join(['\t\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
            time.sleep(retry_sleep_sec)
            twitter_conn.reconnect()
            continue
        except Exception as e:
            logging.error(''.join(['\t\tError retrieving data for user ', twitter_userid, '. Error message: ', str(e), ' Aborting user timeline ...']))
            traceback.print_exc()
            logging.error('Exiting on error ...')
            twitter_conn.cleanup()
            sys.exit(1)
        retry = False

    logging.info('\tRetriving friends ...')
    retry = True
    while retry:
        try:
            temp = twitter_conn.get_friends(user['screen_name'])
        except twitter.TwitterUserNotFoundException as tunfe:
            logging.warning(''.join(['\t\t', str(tunfe), ' Aborting friends list ...']))
        except twitter.TwitterUserSuspendedException as tuse:
            logging.warning(''.join(['\t\t', str(tuse), ' Aborting friends list ...']))
        except twitter.ProtectedTweetsException as pte:
            logging.warning(''.join(['\t\t', str(pte), ' Aborting friends list ...']))
        except twitter.TwitterServerErrorException as tsee:
            retry_sleep_sec = 60
            logging.warning(''.join(['\t\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
            time.sleep(retry_sleep_sec)
            twitter_conn.reconnect()
            continue
        except Exception as e:
            logging.error(''.join(['\t\tError retrieving data for user ', twitter_userid, '. Error message: ', str(e), ' Aborting friends list ...']))
            traceback.print_exc()
            logging.error('Exiting on error ...')
            twitter_conn.cleanup()
            sys.exit(1)
        retry = False
    temp = filter_twitter_high_connected(temp, connectivity_threshold)
    friends = []
    for friend in temp:
        friends.append({
                        'id'            : friend['id'],
                        'screen_name'   : friend['screen_name'], 
                       })
    with open(os.sep.join([dest_dir, 'friends.json']), mode='wt', encoding='ascii') as fd:
        json.dump(friends, fd, indent=4, sort_keys=True)

    logging.info('\tRetriving friends of friends ...')
    fof = {}    # friends of friends
    i = 0
    for friend in friends:
        i += 1
        logging.debug('{}/{}'.format(i, len(friends)))
        retry = True
        while retry:
            try:
                temp = twitter_conn.get_friends(friend['screen_name'])
            except twitter.TwitterUserNotFoundException as tunfe:
                logging.warning(''.join(['\t\t', str(tunfe), ' Aborting friends list for user ', friend['screen_name'], ' ...']))
            except twitter.TwitterUserSuspendedException as tuse:
                logging.warning(''.join(['\t\t', str(tuse), ' Aborting friends list for user ', friend['screen_name'], ' ...']))
            except twitter.ProtectedTweetsException as pte:
                logging.warning(''.join(['\t\t', str(pte), ' Aborting friends list for user ', friend['screen_name'], ' ...']))
            except twitter.TwitterServerErrorException as tsee:
                retry_sleep_sec = 60
                logging.warning(''.join(['\t\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
                time.sleep(retry_sleep_sec)
                twitter_conn.reconnect()
                continue
            except Exception as e:
                logging.error(''.join(['\t\tError retrieving data for user ', friend['screen_name'], '. Error message: ', str(e), ' Aborting friends list for user ', friend['screen_name'], ' ...']))
                traceback.print_exc()
                logging.error('Exiting on error ...')
                twitter_conn.cleanup()
                sys.exit(1)
            fof[friend['screen_name']] = []
            for element in temp:
                fof[friend['screen_name']].append({
                                                   'id'            : element['id'],
                                                   'screen_name'   : element['screen_name'],
                                                  })
            retry = False
    with open(os.sep.join([dest_dir, 'friends_of_friends.json']), mode='wt', encoding='ascii') as fd:
        json.dump(fof, fd, indent=4, sort_keys=True)

    logging.info('\tRetriving followers of friends ...')
    fwof = {}  # followers of friends
    i = 0
    for friend in friends:
        i += 1
        logging.debug('{}/{}'.format(i, len(friends)))
        retry = True
        while retry:
            try:
                temp = twitter_conn.get_followers(friend['screen_name'])
            except twitter.TwitterUserNotFoundException as tunfe:
                logging.warning(''.join(['\t\t', str(tunfe), ' Aborting followers list for user ', friend['screen_name'], ' ...']))
            except twitter.TwitterUserSuspendedException as tuse:
                logging.warning(''.join(['\t\t', str(tuse), ' Aborting followers list for user ', friend['screen_name'], ' ...']))
            except twitter.ProtectedTweetsException as pte:
                logging.warning(''.join(['\t\t', str(pte), ' Aborting followers list for user ', friend['screen_name'], ' ...']))
            except twitter.TwitterServerErrorException as tsee:
                retry_sleep_sec = 60
                logging.warning(''.join(['\t\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
                time.sleep(retry_sleep_sec)
                twitter_conn.reconnect()
                continue
            except Exception as e:
                logging.error(''.join(['\t\tError retrieving data for user ', friend['screen_name'], '. Error message: ', str(e), ' Aborting friends list for user ', friend['screen_name'], ' ...']))
                traceback.print_exc()
                logging.error('Exiting on error ...')
                twitter_conn.cleanup()
                sys.exit(1)
            fwof[friend['screen_name']] = []
            for element in temp:
                fwof[friend['screen_name']].append({
                                                    'id'            : element['id'],
                                                    'screen_name'   : element['screen_name'],
                                                   })
            retry = False
    with open(os.sep.join([dest_dir, 'followers_of_friends.json']), mode='wt', encoding='ascii') as fd:
        json.dump(fwof, fd, indent=4, sort_keys=True)

    return {'user'                  : user,
            'tweets'                : tweets_retweets,
            'friends'               : friends,
            'friends_of_friends'    : fof,
            'followers_of_friends'  : fwof,
           }


if __name__ == '__main__':
    # parsing arguments
    args = command_line_parsing()

    # logging configuration
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s')

    logging.info(''.join(['Starting Twitter Analizer with the following parameters:',
                          '\n\tUser id = ', args.user_id,
                          '\n\tDestination directory = ', args.dest_dir,
                          '\n\tHigh connection threshold = ', str(args.high_connection_threshold),
                          '\n\tDebug = ', str(args.debug),
                         ]))

    logging.info('Creating output directory ...')
    if os.path.exists(args.dest_dir):
        logging.error(''.join(['Destination directory ', args.dest_dir, ' already exists. Quitting ...']))
        sys.exit(1)
    os.makedirs(args.dest_dir)

    logging.info('Collecting Twitter data ...')
    twitter_data = retrieve_twitter_data(args.user_id, args.dest_dir, args.high_connection_threshold)

    logging.info('Finished.')
