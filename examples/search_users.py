#!/usr/bin/env python3


# Code to search Twitter users based on a list of words (usually function
#   words). The recovered users are stored in a file containing a line per user
#   in the format "id screen_name". All the remaining user information is
#   stored in a directory named 'full_info' with a file per user in JSON format.
#
# It is recommended not to use too small function words (less than 3 characters)
#   since they can match undesired languages.
#
# Pseudo-code:
#   read list of words
#   for each 'word':
#       get a list of recent users that used this word 
#       save user information if not already seen
#
# References:
#   [1] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json
#   [2] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
#   [3] https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object


import sys
sys.path.append('..')
import argparse
import logging
import traceback
import os
import twitter
import json


def command_line_parsing():
    parser = argparse.ArgumentParser()
    parser.add_argument('--words-file-name', '-w',
                        dest='words_file_name',
                        required=True,
                        help='File name with the words to be used in the users\' search.')
    parser.add_argument('--destination-dir', '-e',
                        dest='destination_dir',
                        required=True,
                        help='Directory name to be created where the collected data will be stored.')
    parser.add_argument('--language', '-l',
                        dest='language',
                        default='en',
                        help='Language of the users to be searched. Default = en.')
    parser.add_argument('--max-results-per-word', '-m',
                        dest='max_results_per_word',
                        type=int,
                        default=1000,
                        help='Maximum number of results in a search for a word. Default = 1000.')
    parser.add_argument('--stop-on-error', '-s',
                        dest='stop_on_error',
                        action='store_true',
                        default=False,
                        help='Stop the collecting if an HTTP error occurs.')
    parser.add_argument('--debug', '-d',
                        dest='debug',
                        type=int,
                        choices = [0, 1, 2],
                        nargs='?',
                        const=1,
                        default=0,
                        help='Print debug information. 0 = no debug (default); 1 = normal debug; 2 = deeper debug (HTTP debug).')
    return parser.parse_args()


if __name__ == '__main__':
    # parsing arguments
    args = command_line_parsing()

    # logging configuration
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s')

    logging.info(''.join(['Starting search for Twitter\'s users with the following parameters:',
                            '\n\twords file name = ',                       args.words_file_name,
                            '\n\tdestination directory = ',                 args.destination_dir,
                            '\n\tlanguage = ',                              args.language,
                            '\n\tmaximum number of results per word = ',    str(args.max_results_per_word),
                            '\n\tstop on error = ',                         str(args.stop_on_error),
                            '\n\tdebug = ',                                 str(args.debug),
                         ]))

    logging.info('Reading words file name ...')
    with open(args.words_file_name, encoding='utf-8') as fd:
        words = fd.read().split()

    logging.info('Creating destination directories ...')
    if os.path.exists(args.destination_dir):
        logging.error('Output directory already exists. Quitting ...')
        sys.exit(1)
    os.mkdir(args.destination_dir)
    full_info_dir = os.sep.join([args.destination_dir, 'full_info'])
    os.mkdir(full_info_dir)

    logging.info('Connecting to Twitter ...')
    app_name        = '<your application name>'
    consumer_key    = '<your application consumer key>'
    consumer_secret = '<your application consumer secret>'
    twitter_conn = twitter.TwitterReader(app_name, consumer_key, consumer_secret, debug_connection = (args.debug == 2) )
    twitter_conn.connect()

    logging.info('Retrieving Twitter\'s users ...')
    final_users = {}
    for word in words:
        retry = True
        while retry:
            logging.debug(''.join(['\tSearching users by word \'', word, '\' ...']))
            try:
                users = twitter_conn.search_users(word, args.language, args.max_results_per_word)
            except twitter.TwitterServerErrorException as tsee:
                retry_sleep_sec = 60
                logging.warning(''.join(['\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
                time.sleep(sleep_sec)
                retry = True
                continue
            except Exception as e:
                logging.error(''.join(['Error trying to search users by word ', word, ' . Error: ', str(e), '. Aborting the search for the word \'', word , '\' ...']))
                traceback.print_exc()
                if args.stop_on_error:
                    logging.error('Exiting on error ...')
                    twitter_conn.cleanup()
                    sys.exit(1)
                twitter_conn.reconnect()
            retry = False
        for user_id in users.keys():
            if user_id not in final_users:
                final_users[user_id] = users[user_id]['screen_name']
                with open(''.join([full_info_dir, os.sep, user_id, '.json']), mode='w', encoding='ascii') as fd:
                    json.dump(users[user_id], fd, sort_keys=True, ensure_ascii=True)
        logging.debug(''.join(['\tCurrent number of users found = ', str(len(final_users.keys())), '.']))

    logging.debug(''.join(['Total number of users found = ', str(len(final_users.keys())), '.']))
    logging.info('Saving users\' list ...')
    final_users_list = []
    for user_id in sorted(final_users.keys()):
        final_users_list.append(' '.join([user_id, final_users[user_id]]))
    with open(os.sep.join([args.destination_dir, 'users.txt']), mode='w', encoding='utf-8') as fd:
        fd.write('\n'.join(final_users_list))

    twitter_conn.cleanup()
    logging.info('Finishing ...')
