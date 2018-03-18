#!/usr/bin/env python3


# Code to download the timeline of a list of Twitter users. All the tweets and
#   metadata is saved in JSON format in a directory for each user.
#
# Pseudo-code:
#   Read file with list of Twitter users to have their timeline downloaded
#   Connect with Twitter
#   For each user to download
#       Retrive user information
#       Download all the tweets in the timeline according to [1]
#       Save the information in JSON format
#


import sys
sys.path.append('..')
import argparse
import logging
import os
import twitter
import traceback
import json
import time


def command_line_parsing():
    parser = argparse.ArgumentParser()
    parser.add_argument('--users-file-name', '-u',
                        dest='users_file_name',
                        required=True,
                        help='File name with information about the users that will have their timeline collected.')
    parser.add_argument('--destination-dir', '-e',
                        dest='destination_dir',
                        required=True,
                        help='Directory name to be created where the collected data will be stored.')
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

    logging.info(''.join(['Starting Twitter\'s timelines collecting with the following parameters:',
                            '\n\tusers file name = ', args.users_file_name,
                            '\n\tdestination directory = ', args.destination_dir,
                            '\n\tstop on error = ', str(args.stop_on_error),
                            '\n\tdebug = ', str(args.debug),
                         ]))

    logging.info('Reading Twitter\'s users information to be collected ...')
    with open(args.users_file_name, encoding='utf-8') as fd:
        users = fd.readlines()

    logging.info(''.join(['Creating destination directory ', args.destination_dir, ' ...']))
    if os.path.exists(args.destination_dir):
        logging.error('Output directory already exists. Quitting ...')
        sys.exit(1)
    os.mkdir(args.destination_dir)

    logging.info('Connecting to Twitter ...')
    app_name        = '<your application name>'
    consumer_key    = '<your application consumer key>'
    consumer_secret = '<your application consumer secret>'
    twitter_conn = twitter.TwitterReader(app_name, consumer_key, consumer_secret, debug_connection = (args.debug == 2) )
    twitter_conn.connect()

    logging.info('Retrieving Tweets ...')
    acc_users = 0
    acc_tweets = 0
    for user in users:
        user_id, user_screen_name = user.split()
        retry = True
        while retry:
            try:
                logging.debug(''.join(['\tRetrieving user information and timeline from user ', user_screen_name, ' , id = ', user_id, ' ...']))
                user_info = twitter_conn.get_user_info(user_id)
                tweets = twitter_conn.get_user_timeline(user_id)
            except twitter.TwitterUserNotFoundException as tunfe:
                logging.warning(''.join(['\t', str(tunfe), ' Aborting user timeline ...']))
            except twitter.TwitterUserSuspendedException as tuse:
                logging.warning(''.join(['\t', str(tuse), ' Aborting user timeline ...']))
            except twitter.ProtectedTweetsException as pte:
                logging.warning(''.join(['\t', str(pte), ' Aborting user timeline ...']))
            except twitter.TwitterServerErrorException as tsee:
                retry_sleep_sec = 60
                logging.warning(''.join(['\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
                time.sleep(retry_sleep_sec)
                retry = True
                continue
            except Exception as e:
                logging.error(''.join(['\tError retrieving data for user ', user_screen_name, ' , id = ', user_id, '. Error message: ', str(e), ' Aborting user timeline ...']))
                traceback.print_exc()
                if args.stop_on_error:
                    logging.error('Exiting on error ...')
                    twitter_conn.cleanup()
                    sys.exit(1)
                twitter_conn.reconnect()
            retry = False

        logging.debug('\tSaving retrieved data ...')
        tweets.reverse()    # put older tweets first
        user_dir = os.sep.join([args.destination_dir, user_id])
        os.mkdir(user_dir)
        with open(os.sep.join([user_dir, 'user.json']), mode='w', encoding='ascii') as fd:
            json.dump(user_info, fd, sort_keys=True, ensure_ascii=True)
        with open(os.sep.join([user_dir, 'tweets.json']), mode='w', encoding='ascii') as fd:
            json.dump(tweets, fd, sort_keys=True, ensure_ascii=True)
        acc_users += 1
        acc_tweets += len(tweets)
        logging.debug(''.join(['\t', str(acc_tweets), ' tweets from ', str(acc_users), ' users retrieved so far.']))

    logging.info(''.join([str(acc_tweets), ' tweets from ', str(acc_users), ' users retrieved.']))
    twitter_conn.cleanup()
    logging.info('Finishing ...')
