# -*- coding:utf-8 -*-
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterError
import time
import logging


class API(object):
    def __init__(self, token=None, proxy=None):
        self.token = token
        self.proxy = proxy
        self.api = None
        self.set_api()

    def set_api(self):
        if self.token:
            self.api = TwitterAPI(consumer_key=self.token.get('consumer_key'),
                                  consumer_secret=self.token.get('consumer_secret'),
                                  access_token_key=self.token.get('access_token'),
                                  access_token_secret=self.token.get('access_token_secret'),
                                  proxy_url=self.proxy)
        return self.api

    def get_user_lookup(self, screen_name=None, account_id=None):  # 待完善
        """
        根据用户的ID或screen_name补全用户的详细信息
        :param screen_name:
        :param account_id:
        :return:
        """
        logging.debug('start:get_user_lookup')
        commands = {}
        if screen_name:
            commands.update({'screen_name': screen_name})
        if account_id:
            commands.update({'user_id': account_id})
        try:
            results = self.api.request('users/lookup', commands)                                         #
            if results.status_code > 400:
                raise TwitterError.TwitterRequestError(results.status_code)    # (待完善)
        except TwitterError.TwitterRequestError as err:
            logging.error(err.message)
        except TwitterError.TwitterConnectionError as err:
            logging.error(err.message)                           #
            time.sleep(15)
        except Exception as err:
            logging.error(err.message)
        else:
            return results

    def get_user_friends(self, user, user_type='friends', cursor=-1):
        """
        获取用户的friends/followers的ID列表
        :param user: 待采集用户的ID
        :param user_type: "friends/followers"
        :param cursor: 采集开始位置
        :return:
        """
        logging.debug('start:get_user_friends/followers')
        commands = {'user_id': user, 'cursor': cursor, 'count': 5000}
        try:
            results = self.api.request(user_type+'/ids', commands)
            if results.status_code > 400:
                raise TwitterError.TwitterRequestError(results.status_code)    # (待完善)
        except TwitterError.TwitterRequestError as err:
            logging.error(err.message)
        except TwitterError.TwitterConnectionError as err:
            logging.error(err.message)                          #
            time.sleep(15)
        else:
            try:
                json_data = results.json()
                users_ids = json_data.get('ids')
            except Exception as err:
                print err
                return cursor
            else:
                return users_ids


class DataBase(object):
    """
    操作数据库
    """
    def __init__(self, host=None, db=None, col=None):
        self.host = host
        self.db = self.host.get_database(db)
        self.col = self.db.get_collection(col)

    def update_data(self, user_id, friends_id):
        """
        更新数据库内容
        :return:
        """
        self.col.update({'id': user_id}, {'$set': {'friends_ids': friends_id}}, upsert=True)
        pass

    def delete_data(self):
        pass
