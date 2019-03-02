# -*- coding:utf-8 -*-
from pymongo import MongoClient

# if __name__ == '__main__':
#     #  将服务器数据库中的token复制到本地数据库中
#     TokenClient = MongoClient('mongodb://mongo:123456@121.49.99.14:27017')
#     token_db = TokenClient.get_database('crawler_statues')
#     col_token = token_db.get_collection('twitter_apis')
#
#     UserClient = MongoClient('localhost')
#     User_db = UserClient.get_database('Token')
#     col_user = User_db.get_collection('user_token')

#     num_token = col_token.count()
#     for x in range(num_token):
#         data = col_token.find_one({}, {'_id': 0, "access_token": 1,
#                                        "consumer_key": 1,
#                                        "consumer_secret": 1,
#                                        "email_pwd": 1,
#                                        "tw_pwd": 1,
#                                        "access_token_secret": 1,
#                                        "email": 1})
#         col_user.insert(data)
#         print data

if __name__ == '__main__':
    # 把一个Twitter用户的ID插入到目标数据库中，作为采集用户的基用户
    UserClient = MongoClient('mongodb://mongo:123456@121.49.99.14:27017')
    User_db = UserClient.get_database('twitter_user_tweet')
    col_user = User_db.get_collection('user')

    LocalClient = MongoClient('localhost')
    Local_db = LocalClient.get_database('Twitter_user')
    New_user = Local_db.get_collection('user_tests')

    data = col_user.find_one({}, {'_id': 0, 'id': 1})
    New_user.insert(data)
