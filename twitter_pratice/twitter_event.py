# -*- coding:utf-8 -*-
# 取数据库中的推文内容，之前的目标服务器是222，实现从一个集合采集ID，来从另一个集合中取出对应ID的数据
from pymongo import MongoClient
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

client = MongoClient('mongodb://121.49.99.14:27017')
# 链接数据库
db = client.KeywordsTweets

# 链接目标集合
Terrorism = db.Terrorism
event = db.event
event_num = event.count()  # 判断集合event下的文档数目

# 擦除之前文本中的内容
txt_file = open('event.txt', 'w+')
txt_file.truncate()
txt_file.close()

#  print number
for i in range(0, event_num):
    for u in event.find().skip(i).limit(1):  # 每次循环跳过已经输出的内容，且只选取一条内容
        id_list = u['tweet_id']
        #  print id_list
        txt_file = open('event.txt', 'a')    # 追加的方式打开文件，不擦除之前的数据
        for x in id_list:
            tweet_text = Terrorism.find_one({'id': x})['text']  # .strip()
            text_only = tweet_text.replace("\n", "")        # 去除换行符
            txt_file.write(str(text_only))
            txt_file.write('\n')
            print text_only                                 # , "\n"
        txt_file.write('\n')
        txt_file.close()
        print "\n"        # 有问题，还是出现两个连续空行（但是在文档中满足一个空行）
