# -*- coding:utf-8 -*-
# 采集用户的followers列表
from twitter_api import *
from pymongo import MongoClient
from random import randint
import time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)-8s  %(message)s',
                    datefmt='%Y %b %d %H:%M:%S',
                    filename='user_followers.log',
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s %(filename)-18s [line:%(lineno)d] %(levelname)-8s  %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


def followers_ids(tokens_client=None, tokens_db=None, col_tokens=None,
                  users_client=None, users_db=None, col_users=None, proxy_link=None):

    token_client = MongoClient(tokens_client)
    token_db = token_client.get_database(tokens_db)
    col_token = token_db.get_collection(col_tokens)

    user_client = MongoClient(users_client)
    user_db = user_client.get_database(users_db)
    col_user = user_db.get_collection(col_users)

    while True:
        col_num = col_token.count()
        skip_num = randint(0, col_num - 1)
        token_list = col_token.find().skip(skip_num).limit(1)
        # print token_list.count()

        for Token in token_list:
            # print "The token:", Token
            # user_id = col_user.find({'followers_ids': {"$exists": False}}).limit(1)
            user_id = col_user.find({'has_expand_followers': {"$exists": False}}).limit(1)
            for x in user_id:
                user_id = x['id']
                print "updating user:", user_id, "'s followers ids"

            api = API(token=Token, proxy=proxy_link)
            followers_id = api.get_user_friends(user=user_id, user_type='followers')
            print "The user's followers' ids as lists:\n", followers_id
            col_user.update({'id': user_id}, {'$set': {'followers_ids': followers_id}}, upsert=True)
            # 将用户的followers插入到数据库中
            if followers_id is not None:
                # [col_user.insert({'id': follower_id}) for follower_id in followers_id]
                [col_user.update({'id': follower_id}, {'$set': {'id': follower_id}}, upsert=True)
                 for follower_id in followers_id]

            col_user.update({'id': user_id}, {'$set': {'has_expand_followers': True}}, upsert=True)
            print time.ctime(), "waiting for a moment......"
            time.sleep(60)

if __name__ == '__main__':
    TokenClient = 'mongodb://mongo:123456@121.49.99.14:27017'
    Token_db = 'crawler_statues'
    col_Token = 'twitter_apis'

    UserClient = 'localhost'
    User_db = 'Twitter_user'
    col_User = 'user_tests'

    Proxy = '127.0.0.1:55648'
    followers_ids(tokens_client=TokenClient, tokens_db=Token_db, col_tokens=col_Token,
                  users_client=UserClient, users_db=User_db, col_users=col_User, proxy_link=Proxy)
