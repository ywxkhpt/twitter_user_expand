# -*- coding:utf-8 -*-
# 采集用户的friends列表
# import pymongo
from twitter_api import *
from pymongo import MongoClient
from random import randint
import time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)-18s [line:%(lineno)d] %(levelname)-8s  %(message)s',
                    datefmt='%Y %b %d %H:%M:%S',
                    filename='user_friends.log',
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s %(filename)-18s [line:%(lineno)d] %(levelname)-8s  %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


def friends_ids(tokens_client=None, tokens_db=None, col_tokens=None,
                users_client=None, users_db=None, col_users=None, proxy_link=None):
    token_client = MongoClient(tokens_client)
    token_db = token_client.get_database(tokens_db)
    col_token = token_db.get_collection(col_tokens)

    user_client = MongoClient(users_client)
    user_db = user_client.get_database(users_db)
    col_user = user_db.get_collection(col_users)
    col_user.ensure_index('id', unique=True)
    # col_user.create_index([('id', pymongo.ASCENDING), ('has_expand_friends', pymongo.DESCENDING)])
    while True:
        # 随机获取一个token
        col_num = col_token.count()
        skip_num = randint(0, col_num - 1)
        token_list = col_token.find().skip(skip_num).limit(1)
        # print token_list.count()

        for Token in token_list:
            # user_id = col_user.find({'friends_ids': {"$exists": False}}).limit(1)
            user_id = col_user.find({'has_expand_friends': {"$exists": False}}).limit(1)
            for x in user_id:
                user_id = x['id']
                print "updating user:", user_id, "'s friends ids"

            api = API(token=Token, proxy=proxy_link)
            friends_id = api.get_user_friends(user=user_id, user_type='friends')
            print "The user's friends' ids as lists:\n", friends_id

            col_user.update({'id': user_id}, {'$set': {'friends_ids': friends_id}}, upsert=True)    # 设置条件？？？
            # 如果用户的friends列表为非空执行以下操作, 插入新用户，更新原用户字段
            if friends_id is not None:
                [col_user.update({'id': friend_id}, {'$set': {'id': friend_id}}, upsert=True)
                 for friend_id in friends_id]   # 避免重复插入用户
                # [col_user.insert({'id': friend_id}) for friend_id in friends_id]
            col_user.update({'id': user_id}, {'$set': {'has_expand_friends': True}}, upsert=True)    #
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
    friends_ids(tokens_client=TokenClient, tokens_db=Token_db, col_tokens=col_Token,
                users_client=UserClient, users_db=User_db, col_users=col_User, proxy_link=Proxy)
