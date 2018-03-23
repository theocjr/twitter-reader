#!/usr/bin/env python3


import sys              # if you include the directory where twitter.py is located in your path you don't need this block
sys.path.append('..')


import twitter
twitter_conn = twitter.TwitterReader('<your Twitter application name>',     # App name
                                     '<application consumer key>',          # consumer key
                                     '<application secret key>'             # secret key
                                     )
twitter_conn.connect()
users = twitter_conn.search_users('about', max_results=100)
user_id = list(users)[0]
user = twitter_conn.get_user_info(user_id)
tweets = twitter_conn.get_user_timeline(user_id)

import json
print(json.dumps(user, indent=4, sort_keys=True))
print()
print(json.dumps(tweets[0], indent=4, sort_keys=True))
