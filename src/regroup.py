# -*- coding: utf-8 -*-
import time, re, requests, copy, os, traceback, json, pprint, logging
from datetime         import datetime, timedelta
from setting.settings import app, db
from model.elastic    import Elastic

def new_logger(logger_name, logfile_path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(logfile_path)
    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s[%(threadName)s][%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logfile_path = './log/[grouping]%s.log' % datetime.today().strftime('%Y-%m-%d')
logger = new_logger('cofacts_article', logfile_path)

es = Elastic(**app.config['FAKENEWS_DATA_ESHOST'])
cofacts_index_name = 'fakenews@cofacts_article'
query_term = {
    'query': {
        'match_all': {}
    }
}

while True:
    try:
        
        scan_result = list(es.scan(query_term, cofacts_index_name))
        logger.info('total data count:%d' % len(scan_result))
        finish_data_count = 0
        for data in scan_result:
            ##set grouping data structure
            article_url = []
            if data['_source']['hyperlinks']:
                for url_info in data['_source']['hyperlinks']:
                    article_url.append(url_info['url'])

            get_group_id_json = {
                'class_name': 'CofactsGrouping',
                'text':data['_source']['text_tc'],
                'url': article_url
            }

            grouping_error_time = 0
            while grouping_error_time < 5:
                response = requests.post('http://10.0.1.108:40000/group_id/', json=get_group_id_json, timeout=600)
                #print (response.json())
                if response.status_code == 200:
                    if response.json()['status']:
                        group_info = response.json()['result']
                        break
                    elif not response.json()['status']:
                        logger.error('Get group id error')
                        logger.error(response.json())
                        grouping_error_time += 1
                else:
                    logger.error('Get group id error')
                    logger.error(response.json())
                    grouping_error_time += 1

            if grouping_error_time == 5:
                logger.error('Get group id error exceed 5 times, retry')
                continue

            group_id = group_info['group_id']
            article_url_content = group_info['url_content']

            article_type = []
            if data['_source']['text']:
                article_type.append('text')
            if data['_source']['hyperlinks']:
                article_type.append('link')

            grouping_data = [
                {
                    '_index': app.config['GROUPING_INDEX'],
                    '_id': 'cofacts_%s' % data['_id'],
                    'group_id': group_id,
                    'source': 'cofacts',
                    'type': article_type,
                    'message': data['_source']['text'],
                    'message_tc': data['_source']['text_tc'],
                    'link': list(article_url_content.keys()),
                    'link_message': list(article_url_content.values()),
                    'file': [],
                    'file_name':[],
                    'create_time': data['_source']['createdAt'],
                    'db_update_time': datetime.now().strftime(app.config['TIMEFORMAT']) 
                }
            ]

            #logger.info(grouping_data)
            es.batch_load(grouping_data)
            finish_data_count += 1
            if finish_data_count % 1000 == 0:
                logger.info('finish group 1000 data')
        logger.info('finish group data')
        break
    except Exception as err:
        logger.error('Grouping error')
        logger.error(str(err))
        continue
