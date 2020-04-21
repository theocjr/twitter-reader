#!/usr/bin/env python3


'''
Code to retrieve tweets from tweet ids. The tweet ids are stored in a file (one
    id per line), and the recovered tweets are stored in a file in JSON format.
'''


import argparse
import logging
import pprint
import sys
sys.path.append('..')
import twitter
import traceback
import time
import json


def command_line_parsing():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--tweet_ids_filename', '-f',
                        required=True,
                        help='File name with the tweet ids to be collected.')
    parser.add_argument('--destination_filename', '-e',
                        required=True,
                        help='JSON filename to be created where the collected data will be stored.')
    parser.add_argument('--stop_on_error', '-s',
                        dest='stop_on_error',
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

    logging.info('Reading tweet ids file name ...')
    with open(args.tweet_ids_filename, encoding='ascii') as fd:
        tweet_ids = [ line.strip() for line in fd ]

    logging.info('Connecting to Twitter ...')
    app_name        = '<your application name>'
    consumer_key    = '<your application consumer key>'
    consumer_secret = '<your application consumer secret>'
    twitter_conn = twitter.TwitterReader(app_name, consumer_key, consumer_secret, debug_connection = (args.debug == 2) )
    twitter_conn.connect()

    logging.info('Retrieving tweets ...')
    retry = True
    while retry:
        try:
            tweets = twitter_conn.hydrate_tweets(tweet_ids, extended=True)
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
    twitter_conn.cleanup()

    with open(args.destination_filename, mode='xt', encoding='ascii') as fd:
        json.dump(tweets, fd, sort_keys=True, ensure_ascii=True)

    logging.info('Finished.')
