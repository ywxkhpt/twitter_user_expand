# -*- coding:utf-8 -*-
# 采集用户的详细信息
from twitter_api import *
from pymongo import MongoClient
from random import randint
import time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)-18s [line:%(lineno)d] %(levelname)-8s  %(message)s',
                    datefmt='%Y %b %d %H:%M:%S',
                    filename='user_details.log',
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s %(filename)-18s [line:%(lineno)d] %(levelname)-8s  %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


def user_detail(tokens_client=None, tokens_db=None, col_tokens=None,
                users_client=None, users_db=None, col_users=None, proxy_link=None, q=None):
    token_client = MongoClient(tokens_client)
    token_db = token_client.get_database(tokens_db)
    col_token = token_db.get_collection(col_tokens)

    user_client = MongoClient(users_client)
    user_db = user_client.get_database(users_db)
    col_user = user_db.get_collection(col_users)

    col_num = col_token.count()
    # skip_num = randint(0, col_num - 1)
    skip_num = 10
    token_list = col_token.find().skip(skip_num).limit(1)

    for Token in token_list:
        print "The token:", Token
        user_id = col_user.find({'screen_name': {"$exists": False}}).limit(1)
        for x in user_id:
            user_id = x['id']
            print "updating user:", user_id, "'s details"

        api = API(token=Token, proxy=proxy_link)
        detail = api.get_twitter_user(q=q)
        if detail is not None:
            for item in detail.get_iterator():
                print 'Number of item:', len(item), "\n", item  #
                # col_user.update({'id': user_id}, {"$set": item}, upsert=True)
                col_user.insert(item)
        print time.ctime(), "waiting for a moment......"
        time.sleep(60)


if __name__ == '__main__':
    TokenClient = 'mongodb://liuchang:liuchang314@121.49.99.14:30011'  # 'localhost'
    Token_db = "Account"  # 'Twitter20190228'  Account
    col_Token = "TwitterToken2019"  # 'token'   TwitterToken

    UserClient = 'localhost'
    User_db = 'twitter_test'
    col_User = 'user_tests'
    Proxy = 'kb111.asuscomm.com:8120'

    user_detail(tokens_client=TokenClient, tokens_db=Token_db, col_tokens=col_Token,
                users_client=UserClient, users_db=User_db, col_users=col_User, proxy_link=Proxy, q="tom")
