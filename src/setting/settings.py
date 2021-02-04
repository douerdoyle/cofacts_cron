# -*- encoding: utf-8 -*-
import time, sys, os
from flask                   import Flask
from flask_sqlalchemy        import SQLAlchemy
sys.path.append('../')

app  = Flask(__name__)

class Config(object):
    DEBUG = True

    #mysql config
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://account:password@ip:port/db?charset=utf8mb4'
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_POOL_TIMEOUT = 20
    SQLALCHEMY_POOL_RECYCLE = 100

    #es config
    FAKENEWS_DATA_ESHOST = {
        'host': [''],
        'username':'',
        'password':''
    }

    GOOGLE_SENDER_CONF = {
        'FROM_ADDRESS':'',
        'FROM_ADDRESS_PSW':'',
        'SMTP_SERVER':'smtp.gmail.com',
        'SMTP_PORT':'587',
    }

    ARTICLE_MAPPING_PATH = './setting/es_mapping/Index_1.json'
    REPLY_MAPPING_PATH = './setting/es_mapping/Index_2.json'
    GROUPING_MAPPING_PATH = './setting/es_mapping/Index_3.json'

    RETRY_LIMIT = 5

    #crawler config
    COFACTS_GRAPHQL_URL = 'https://cofacts-api.g0v.tw/graphql'
    QUERY_SIZE = 500
    ARTICLE_QUERY_JSON = {
        'operationName': 'ListArticles',
        'variables': {
          'filter': {
            'replyRequestCount': {
              'GT': -1
            }
          },
          'orderBy': [
            {
              'lastRequestedAt': 'ASC'
            }
          ],
          'before': '',
          'after': ''
        },
        'query': 'query ListArticles($filter: ListArticleFilter, $orderBy: [ListArticleOrderBy], $before: String, $after: String) {  ListArticles(filter: $filter, orderBy: $orderBy, before: $before, after: $after, first: %d) {totalCount    edges {      node {        ...ArticleItem     }      cursor    }    }}fragment ArticleItem on Article {  id  text  replyCount replyRequestCount  lastRequestedAt  createdAt updatedAt articleReplies {replyId articleId user{id name} canUpdateStatus feedbackCount positiveFeedbackCount negativeFeedbackCount feedbacks{id user{id name} comment score} status createdAt updatedAt} replyRequests {id userId appId reason feedbackCount positiveFeedbackCount negativeFeedbackCount createdAt updatedAt} hyperlinks{url normalizedUrl title summary topImageUrl fetchedAt status error}}' % QUERY_SIZE
    }

    REPLY_QUERY_JSON = {
        'operationName': 'ListReplies',
        'variables': {
            'orderBy': [
                {
                    'createdAt': 'ASC'
                }
            ],
            'before': '',
            'after': ''
        },
        'query': 'query ListReplies($filter: ListReplyFilter, $orderBy: [ListReplyOrderBy], $before: String, $after: String) {  ListReplies(filter: $filter, orderBy: $orderBy, before: $before, after: $after, first: %d) {totalCount    edges {      node {        ...ReplyItem      }      cursor }      }}fragment ReplyItem on Reply {  id  text  type  createdAt  user {   id name } reference articleReplies {replyId articleId user{id name} canUpdateStatus feedbackCount positiveFeedbackCount negativeFeedbackCount feedbacks{id user{id name} comment score} status createdAt updatedAt} hyperlinks{url normalizedUrl title summary topImageUrl fetchedAt status error}}' % QUERY_SIZE
    }

    TIMEFORMAT = '%Y-%m-%d %H:%M:%S'

def formal_settings_154():
    app.config['RUMOR_GROUPING_API_URL'] = ''

    app.config['ARTICLE_INDEX'] = ''
    app.config['REPLY_INDEX'] = ''
    app.config['GROUPING_INDEX'] = ''

def formal_settings_155():
    app.config['RUMOR_GROUPING_API_URL'] = ''

    app.config['ARTICLE_INDEX'] = ''
    app.config['REPLY_INDEX'] = ''
    app.config['GROUPING_INDEX'] = ''

def formal_settings():
    app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'] = [
        ''
    ]
    app.config['LINE_NOTIFY_TOKEN'] = ''# 不實訊息快篩平台-Cofacts爬蟲
    formal_init_dict = {
        '154':formal_settings_154,
        '155':formal_settings_155
    }
    formal_init_dict[os.environ['SERIAL']]()

def dev_settings():
    app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'] = [
        '',
    ]

    app.config['LINE_NOTIFY_TOKEN'] = '' # douer-ddd

    app.config['RUMOR_GROUPING_API_URL'] = ''

    app.config['ARTICLE_INDEX'] = ''
    app.config['REPLY_INDEX'] = ''
    app.config['GROUPING_INDEX'] = ''

def general_settings():
    pass

app.config.from_object('setting.settings.Config')

dynamic_settings = {
    'FORMALITY':formal_settings,
    'DEV'      :dev_settings
}
dynamic_settings[os.environ.get('API_PROPERTY')]()

general_settings()
app.url_map.strict_slashes = False

db = SQLAlchemy(app)