#!/usr/bin/env python3


'''
Code to search Twitter for tweets based on a list of expressions. The
    recovered tweets are stored in a file per expression in JSON format with the
    filename pattern <id>.json where <id> is the expression id indicated by the
    file expression_ids.txt .
'''


import sys
sys.path.append('..')
import argparse
import logging
import traceback
import os
import twitter
import time
import json


def command_line_parsing():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--expressions-file-name', '-f',
                        dest='expressions_file_name',
                        required=True,
                        help='File name with the expressions to be collected.')
    parser.add_argument('--destination-dir', '-e',
                        dest='destination_dir',
                        required=True,
                        help='Directory name to be created where the collected data will be stored.')
    parser.add_argument('--language', '-l',
                        dest='language',
                        default='en',
                        help='Language to filter the tweets. Default = en.')
    parser.add_argument('--max-results-per-expression', '-m',
                        dest='max_results_per_expression',
                        type=int,
                        default=0,
                        help='Maximum number of results in a search for a expression. Default = 0 (no limit).')
    parser.add_argument('--stop-on-error', '-s',
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

    logging.info(''.join(['Starting collecting Twitter data with the following parameters:',
                            '\n\texpressions file name = ',                     args.expressions_file_name,
                            '\n\tdestination directory = ',                     args.destination_dir,
                            '\n\tlanguage = ',                                  args.language,
                            '\n\tmaximum number of results per expression = ',  str(args.max_results_per_expression),
                            '\n\tstop on error = ',                             str(args.stop_on_error),
                            '\n\tdebug = ',                                     str(args.debug),
                         ]))

    logging.info('Reading expressions file name ...')
    with open(args.expressions_file_name, encoding='utf-8') as fd:
        exprs = [ line.strip().lower() for line in fd ]     # Twitter makes no case disctintion in its search API

    logging.info('Creating destination directory ...')
    if os.path.exists(args.destination_dir):
        logging.error('Output directory already exists. Quitting ...')
        sys.exit(1)
    os.mkdir(args.destination_dir)

    expr_ids = {}
    for expr in exprs:
        expr_ids[expr] = len(expr_ids)
    with open(os.sep.join([args.destination_dir, 'expression_ids.txt']), mode='xt', encoding='utf-8') as fd:
        for expr in sorted(expr_ids.keys()):
            fd.write('{} - \'{}\'\n'.format(expr_ids[expr], expr))

    logging.info('Connecting to Twitter ...')
    app_name        = '<your application name>'
    consumer_key    = '<your application consumer key>'
    consumer_secret = '<your application consumer secret>'
    twitter_conn = twitter.TwitterReader(app_name, consumer_key, consumer_secret, debug_connection = (args.debug == 2) )
    twitter_conn.connect()

    logging.info('Retrieving tweets ...')
    for expr in exprs:
        retry = True
        while retry:
            logging.debug(''.join(['\tSearching tweets by expression \'', expr, '\' , id = ', str(expr_ids[expr]), '...']))
            try:
                tweets = twitter_conn.search_expression(expr, args.language, args.max_results_per_expression)
            except twitter.TwitterServerErrorException as tsee:
                retry_sleep_sec = 60
                logging.warning(''.join(['\t', str(tsee), ' Sleeping for ', str(retry_sleep_sec), ' seconds and retrying ...']))
                time.sleep(retry_sleep_sec)
                continue
            except Exception as e:
                logging.error(''.join(['Error trying to search tweets by expression ', expr, ' . Error: ', str(e), ' Aborting the search for the expression \'', expr , '\' ...']))
                traceback.print_exc()
                if args.stop_on_error:
                    logging.error('Exiting on error ...')
                    twitter_conn.cleanup()
                    sys.exit(1)
                twitter_conn.reconnect()
            retry = False

        with open(os.sep.join([args.destination_dir, str(expr_ids[expr]) + '.json']), mode='xt', encoding='ascii') as fd:
            json.dump(tweets, fd, sort_keys=True, ensure_ascii=True)

    twitter_conn.cleanup()
    logging.info('Finished.')
