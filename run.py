# -*- coding:utf-8 -*-

from twitter_user_details import user_detail


def run(name):
    TokenClient = 'mongodb://liuchang:liuchang314@121.49.99.14:30011'  # 'localhost'
    Token_db = "Account"  # 'Twitter20190228'  Account
    col_Token = "TwitterToken2019"  # 'token'   TwitterToken

    UserClient = 'localhost'
    User_db = 'twitter_test'
    col_User = 'user_tests'
    Proxy = 'kb111.asuscomm.com:8120'

    user_detail(tokens_client=TokenClient, tokens_db=Token_db, col_tokens=col_Token,
                users_client=UserClient, users_db=User_db, col_users=col_User, proxy_link=Proxy, q=name)


if __name__ == "__main__":
    run("tony")
