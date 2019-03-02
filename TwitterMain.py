# -*- coding:utf-8 -*-
# 采集用户的friends,followers及详细信息
from twitter_api import *
from pymongo import MongoClient
from random import randint
import time
import threading
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)-8s  %(message)s',
                    datefmt='%Y %b %d %H:%M:%S',
                    filename='twitter_main.log',
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
                print "The %s's friends' ids as lists:\n" % user_id, friends_id
                col_user.update({'id': user_id}, {'$set': {'friends_ids': friends_id}}, upsert=True)
                # 如果用户的friends列表为非空执行以下操作, 插入新用户，更新原用户字段
                if friends_id is not None:
                    [col_user.update({'id': friend_id}, {'$set': {'id': friend_id}}, upsert=True)
                     for friend_id in friends_id]   # 避免重复插入用户
                    # [col_user.insert({'id': friend_id}) for friend_id in friends_id]

                col_user.update({'id': user_id}, {'$set': {'has_expand_friends': True}}, upsert=True)  #
                print time.ctime(), "friends_id, waiting for a moment......"
                time.sleep(60)


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
            # while True:

            for Token in token_list:
                # print "The token:", Token
                # user_id = col_user.find({'followers_ids': {"$exists": False}}).limit(1)
                user_id = col_user.find({'has_expand_followers': {"$exists": False}}).limit(1)
                for x in user_id:
                    user_id = x['id']
                    print "updating user:", user_id, "'s followers ids"

                api = API(token=Token, proxy=proxy_link)
                followers_id = api.get_user_friends(user=user_id, user_type='followers')
                print "The %s's followers' ids as lists:\n" % user_id, followers_id    #
                col_user.update({'id': user_id}, {'$set': {'followers_ids': followers_id}}, upsert=True)
                # 将用户的followers插入到数据库中
                if followers_id is not None:
                    # [col_user.insert({'id': follower_id}) for follower_id in followers_id]
                    [col_user.update({'id': follower_id}, {'$set': {'id': follower_id}}, upsert=True)
                     for follower_id in followers_id]

                col_user.update({'id': user_id}, {'$set': {'has_expand_followers': True}}, upsert=True)
                print time.ctime(), "followers_id, waiting for a moment......"
                time.sleep(60)


def user_detail(tokens_client=None, tokens_db=None, col_tokens=None,
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

            for Token in token_list:
                # print "The token:", Token
                user_id = col_user.find({'screen_name': {"$exists": False}}).limit(1)
                for x in user_id:
                    user_id = x['id']
                    print "updating user:", user_id, "'s details"

                api = API(token=Token, proxy=proxy_link)
                detail = api.get_user_lookup(account_id=user_id)
                if detail is not None:
                    for item in detail.get_iterator():
                        print 'Number of item:', len(item), "\n", item  #
                        col_user.update({'id': user_id}, {"$set": item}, upsert=True)
                print time.ctime(), "user_detail, waiting for a moment......"
                time.sleep(60)


if __name__ == '__main__':
    TokenClient = 'mongodb://mongo:123456@121.49.99.14:27017'
    Token_db = 'crawler_statues'
    col_Token = 'twitter_apis'

    UserClient = 'localhost'
    User_db = 'Twitter_user'
    col_User = 'user_tests'

    Proxy = '127.0.0.1:55648'

    threads = []
    t1 = threading.Thread(target=friends_ids, args=(TokenClient, Token_db, col_Token,
                                                    UserClient, User_db, col_User, Proxy))
    threads.append(t1)
    t2 = threading.Thread(target=followers_ids, args=(TokenClient, Token_db, col_Token,
                                                      UserClient, User_db, col_User, Proxy))
    threads.append(t2)
    t3 = threading.Thread(target=user_detail, args=(TokenClient, Token_db, col_Token,
                                                    UserClient, User_db, col_User, Proxy))
    threads.append(t3)

    for t in threads:
        t.setDaemon(True)   # 将线程声明为守护线程
        t.start()
    t.join()  # 在子线程完成运行之前，这个子线程的父线程将一直被阻塞
