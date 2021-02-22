#!/usr/bin/env python3


'''
'''


import os
import sys
sys.path.append(os.sep.join([os.path.dirname(os.path.abspath(__file__)), '..']))

import argparse
import logging
import pprint
import math
import twitter
import traceback
import time
import gzip
import json
import pickle


def command_line_parsing():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--user_ids_filename', '-f',
                        required=True,
                        help='Filename with the user ids to be collected.')
    parser.add_argument('--destination_directory', '-e',
                        required=True,
                        help='Directory to be created where the collected data will be stored.')
    parser.add_argument('--credentials_filename', '-c',
                        required=True,
                        help='Filename (in JSON format) with the Twitter credentials.')
    parser.add_argument('--stop_on_error', '-s',
                        dest='stop_on_error',
                        action='store_true',
                        default=False,
                        help='Stop the collecting if an HTTP error occurs. Default = no stop.')
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

    logging.info('Starting collecting Twitter data with the following parameters:\n{}'.format(pprint.pformat(vars(args))))

    if os.path.exists(args.destination_directory):
        logging.error(''.join(['Destination directory ', args.destination_directory, ' already exists. Quitting ...']))
        sys.exit(1)
    os.makedirs(args.destination_directory)

    logging.info('Reading credentials data ...')
    with open(args.credentials_filename, mode= 'rt', encoding='ascii') as fd:
        credentials = json.load(fd)

    logging.info('Connecting to Twitter ...')
    twitter_conn = twitter.TwitterReader(credentials['app_name'],
                                         credentials['consumer_key'],
                                         credentials['consumer_secret'],
                                         user_auth=True,
                                         access_token=credentials['access_token'],
                                         access_token_secret=credentials['access_token_secret'],
                                         debug_connection = (args.debug == 2),
                                        )
    twitter_conn.connect()

    logging.info('Reading user IDs ...')
    user_ids = []
    with open(args.user_ids_filename, mode='rt', encoding='ascii') as fd:
        for line in fd:
            user_ids.append(line.strip())

    logging.info('Retrieving Twitter data ...')
    user_location_map = {}
    step_count = 0
#    step_size = 900*100     # Twitter 15-minutes window size
    step_size = 450*100
    num_steps = math.ceil(len(user_ids)/step_size)
    for idx in range(0, len(user_ids), step_size):

        retry = True
        while retry:
            logging.debug('\tRetrieving users (step {}/{}) ...'.format(step_count+1, num_steps))
            try:
                users = twitter_conn.get_users_info(user_ids[idx:idx+step_size])
            except Exception as e:
                logging.error('Error trying to retrieve tweets. Error: {}'.format(e))
                traceback.print_exc()
                if args.stop_on_error:
                    logging.error('Exiting on error ...')
                    twitter_conn.cleanup()
                    sys.exit(1)
                retry_sleep_sec = 60
                logging.warning('Sleeping for {} seconds and retrying ...'.format(retry_sleep_sec))
                time.sleep(retry_sleep_sec)
                twitter_conn.reconnect()
                continue
            retry = False

            for user in users:
                if 'location' in user:
                    user_location_map[user['id']] = user['location']

            logging.debug('\tSaving data (step {}/{}) ...'.format(step_count+1, num_steps))
            destination_filename = os.path.join(args.destination_directory, str(step_count) + '.json.gz')
            with gzip.open(destination_filename, mode='xt', encoding='utf-8') as fd:
                json.dump(users, fd, sort_keys=True, ensure_ascii=True)
            with open(os.path.join(args.destination_directory, 'user_location_map.pkl'), mode='wb') as fd:
                pickle.dump(user_location_map, fd)
            del users
            step_count += 1

    twitter_conn.cleanup()

    logging.info('Finished.')
