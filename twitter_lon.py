# -*- coding:utf-8 -*-
# 取数据库中的推文内容（修改后的程序------>>>new_lon.py）
import pymongo
import time
from pymongo import MongoClient
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

client = MongoClient('mongodb://mongo:123456@222.197.180.150:27017')
# 链接数据库
db = client.KeywordsTweets

# 链接目标集合
Lon = db.Lon

# 擦除之前文本中的内容
txt_file = open('Lon.txt', 'w+')
txt_file.truncate()
txt_file.close()

lon_list = []
for t in Lon.find({}, {"created_at": 1, "text": 1, "user": 1, "_id": 0}):  #.sort([("created_at", 1)]):
    created_time = t['created_at']             # 推文创建时间
    screen = t['user']['screen_name']         # 数据库下嵌套的字典user下的screen_name
    lon_text = t['text'].replace("\n", "")    # 推文内容

    timeArray = time.strptime(created_time, "%a %b %d %H:%M:%S +0000 %Y")     # 转换为时间数组
    timeStamp = int(time.mktime(timeArray))    # 转换为时间戳(int型，便于比较大小)
    lon_dic = {'timeStamp': timeStamp, 'created_time': created_time, 'screen': screen, 'lon_text': lon_text}
    #print dic
    lon_list.append(lon_dic)   # 把各个“字典”数据依次添加到列表中去

lon_list.sort(key=lambda x: x['timeStamp'], reverse=False)   # 对列表中的字典，按照 timeStamp 属性进行排序
for i in lon_list:
    # 输出显示到屏幕（控制screen的长度，以对齐输出）
    print i['created_time'], '\t', "%-18s" % (i['screen']), 'text:', i['lon_text']
    # 写入文本中
    txt_file = open('Lon.txt', 'a')
    txt_file.write(str(i['created_time']+ '\t'+ "%-18s" % (i['screen']) + 'text:'+ i['lon_text']))
    txt_file.write('\n')

txt_file.close()







