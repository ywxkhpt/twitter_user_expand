# -*- coding:utf-8 -*-
# 官方文档链接：https://geduldig.github.io/TwitterAPI/
from TwitterAPI import TwitterAPI
from pymongo import MongoClient
from random import randint

if __name__ == '__main__':
    client = MongoClient('mongodb://mongo:123456@121.49.99.14:27017')
    db = client.crawler_statues
    col = db.twitter_apis
    num = col.count()
    num2 = randint(0, num-1)
    # token = col.find_one()
    token_list = col.find().skip(num2).limit(1)
    # print token_list.count()
    for token in token_list:
        # print token
        api = TwitterAPI(consumer_key=token['consumer_key'],
                         consumer_secret=token['consumer_secret'],
                         access_token_key=token['access_token'],
                         access_token_secret=token['access_token_secret'],
                         proxy_url='127.0.0.1:55648')
        # 获取用户详细信息
        detail = api.request('users/lookup', {'screen_name': 'China_lijianbo'})
        # request('search/tweets', {'q': 'pizza'})
        print detail.status_code, '\n', detail.text
        print "****************************"

        # 使用迭代器输出
        for item in detail.get_iterator():
            # text = item['text'].replace('\n', '')
            print item, "\n", len(item)
            for x in item:
                print x, ':', item[x]
            # print item['created_at'], item['id'], item['user']['screen_name'], ">>>", text

        # 获取用户friends列表
        friends_id = api.request('friends/ids', {'screen_name': 'China_lijianbo'})
        json_data = friends_id.json()
        user_ids = json_data.get('ids')
        print "friends_ids:", user_ids
