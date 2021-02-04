# -*- coding: utf-8 -*-
import time, logging, sys
from datetime import datetime
from cofacts_functions import *

if __name__ == '__main__':
    data_type = sys.argv[1]
    if data_type == 'article':
        sync_article_list()
    elif data_type == 'reply':
        sync_reply_list()
    elif data_type == 'all':
        sync_article_list()
        sync_reply_list()
    else:
        raise(Exception('輸入的參數不正確'))