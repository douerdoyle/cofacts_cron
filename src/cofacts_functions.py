# -*- coding: utf-8 -*-
import time, re, requests, copy, os, traceback, json, pprint, logging, uuid, sys
from copy              import deepcopy
from datetime          import datetime, timedelta
from pprint            import pprint
from sqlalchemy        import or_, and_
from setting.settings  import app, db
from lib.convert       import isodate_to_local, convert_to_tc
from lib.email_sender  import GmailSender
from lib.tools         import check_duplicate_process
from model.models      import Article, Reply, Reply_requests, Reference, Article_reply, Article_reply_feedback
from model.elastic     import Elastic
from collections       import OrderedDict
from lib.line_notify   import LineNotifyManager

reply_type_code = {
    'RUMOR': 1,
    'NOT_RUMOR': 2,
    'OPINIONATED': 3,
    'NOT_ARTICLE': 4
}

lnm = LineNotifyManager(app.config['LINE_NOTIFY_TOKEN'])
# lnm.send_msg('msg')

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

def sync_article_list():
    try:
        script_name = sys.argv[1] if len(sys.argv)>1 else 'article'
        if check_duplicate_process(script_name):
            print(f'{script_name} :尚有相同的程式正在執行')
            return

        logfile_path = './log/[article]{}.log'.format(datetime.today().strftime('%Y-%m-%d'))
        logger = new_logger('cofacts_article', logfile_path)
        es = Elastic(**app.config['FAKENEWS_DATA_ESHOST'])

        if not es.check_index_exist(app.config['ARTICLE_INDEX']):
            es.create_index(app.config['ARTICLE_INDEX'], app.config['ARTICLE_MAPPING_PATH'])
        if not es.check_index_exist(app.config['GROUPING_INDEX']):
            es.create_index(app.config['GROUPING_INDEX'], app.config['GROUPING_MAPPING_PATH'])

        logger.info('Start sync article data')

        err_msg_list = []
        finish_status = False
        createdAt_start = None
        while not finish_status:
            if not createdAt_start:
                es_result = es.search({"from": 0, "size": 1, "sort":[{"createdAt":{"order":"desc"}}]}, app.config['ARTICLE_INDEX'])
                if not es_result['hits']['hits']:
                    createdAt_start = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                else:
                    # 因為Cofacts的API createdAt時區是格林威治時間，故要再減8小時
                    if (datetime.strptime(es_result['hits']['hits'][0]['_source']['createdAt'], '%Y-%m-%d %H:%M:%S')-timedelta(hours=8))<(datetime.now()-timedelta(days=7, hours=8)):
                        createdAt_start = (datetime.strptime(es_result['hits']['hits'][0]['_source']['createdAt'], '%Y-%m-%d %H:%M:%S')-timedelta(hours=8))
                    else:
                        createdAt_start = (datetime.now()-timedelta(days=7, hours=8))
            print(createdAt_start)
            input_json = deepcopy(app.config['ARTICLE_QUERY_JSON'])
            input_json['variables']['filter']['createdAt'] = {'GT':createdAt_start.strftime('%Y-%m-%dT%H:%M:%S.%f')}
            retry_n = 0
            while retry_n<=app.config['RETRY_LIMIT']:
                retry_n+=1
                try:
                    db.session.rollback()
                    db.session.close()
                    rsp = requests.post(app.config['COFACTS_GRAPHQL_URL'], json=input_json)
                    rsp.encoding='utf-8'
                    if rsp.status_code!=200:
                        logger.error('Request API error')
                        logger.error(rsp.text)
                        time.sleep(5)
                        continue
                    rsp_result = rsp.json()
                    if not rsp_result['data']['ListArticles']['edges']:
                        finish_status = True
                        break
                    createdAt_start = datetime.strptime(rsp_result['data']['ListArticles']['edges'][-1]['node']['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if os.environ['API_PROPERTY']=='FORMALITY':
                        conds = [Article.articleId==data['node']['id'] for data in rsp_result['data']['ListArticles']['edges']]
                        db_result_dict = {db_result.articleId:db_result for db_result in Article.query.filter(or_(*conds)).all()}
                        for article_dict in rsp_result['data']['ListArticles']['edges']:
                            if article_dict['node']['id'] not in db_result_dict:
                                db_result_dict[article_dict['node']['id']] = Article()
                                db_result_dict[article_dict['node']['id']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                db_result_dict[article_dict['node']['id']].articleId = article_dict['node']['id']

                            db_result_dict[article_dict['node']['id']].text = article_dict['node'].get('text', None)
                            db_result_dict[article_dict['node']['id']].replyRequestCount = article_dict['node'].get('replyRequestCount', 0)
                            db_result_dict[article_dict['node']['id']].replyCount = article_dict['node'].get('replyCount', 0)
                            db_result_dict[article_dict['node']['id']].lastRequestedAt = isodate_to_local(article_dict['node'], 'lastRequestedAt')
                            db_result_dict[article_dict['node']['id']].createdAt = isodate_to_local(article_dict['node'], 'createdAt')
                            db_result_dict[article_dict['node']['id']].updatedAt = isodate_to_local(article_dict['node'], 'updatedAt')
                            db_result_dict[article_dict['node']['id']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT']) 
                            db.session.add(db_result_dict[article_dict['node']['id']])
                            if article_dict['node']['articleReplies']:
                                articleReply_dict = {
                                    article_dict['node']['id']:{}
                                }
                                conds = [Article_reply.replyId==data['replyId'] for data in article_dict['node']['articleReplies']]
                                for db_result in Article_reply.query.filter(Article_reply.articleId==article_dict['node']['id'], or_(*conds)).all():
                                    if db_result.articleId not in articleReply_dict:
                                        articleReply_dict[db_result.articleId] = {}
                                    articleReply_dict[db_result.articleId][db_result.replyId] = db_result

                                for article_reply in article_dict['node']['articleReplies']:
                                    if article_reply['articleId'] not in articleReply_dict:
                                        articleReply_dict[article_reply['articleId']] = {}
                                    if article_reply['replyId'] not in articleReply_dict[article_reply['articleId']]:
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']] = Article_reply()
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']].articleId = article_reply['articleId']
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']].replyId = article_reply['replyId']
                                    if 'user' in article_reply and article_reply['user']:
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']].userId = article_reply['user'].get('id', None)
                                        articleReply_dict[article_reply['articleId']][article_reply['replyId']].user = article_reply['user'].get('name', None)

                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].negativeFeedbackCount = article_reply.get('negativeFeedbackCount', 0)
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].positiveFeedbackCount =article_reply.get('positiveFeedbackCount', 0)
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].status = article_reply.get('status', None)
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].canUpdateStatus = article_reply.get('canUpdateStatus', None)
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].createdAt = isodate_to_local(article_reply, 'createdAt')
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].updatedAt = isodate_to_local(article_reply, 'updatedAt')
                                    articleReply_dict[article_reply['articleId']][article_reply['replyId']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    db.session.add(articleReply_dict[article_reply['articleId']][article_reply['replyId']])

                                    if article_reply['feedbacks']:
                                        conds = [Article_reply_feedback.articleReplyFeedbackId==article_reply_feedback['id'] for article_reply_feedback in article_reply['feedbacks']]
                                        article_reply_feedback_dict = {db_result.articleReplyFeedbackId:db_result for db_result in Article_reply_feedback.query.filter(or_(*conds)).all()}

                                        for article_reply_feedback in article_reply['feedbacks']:
                                            if article_reply_feedback['id'] not in article_reply_feedback_dict:
                                                article_reply_feedback_dict[article_reply_feedback['id']] = Article_reply_feedback()
                                                article_reply_feedback_dict[article_reply_feedback['id']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                                article_reply_feedback_dict[article_reply_feedback['id']].articleReplyFeedbackId = article_reply_feedback['id']
                                            article_reply_feedback_dict[article_reply_feedback['id']].articleId = article_reply_feedback['id'].split('__')[0]
                                            article_reply_feedback_dict[article_reply_feedback['id']].replyId = article_reply_feedback['id'].split('__')[1]
                                            if 'user' in article_reply_feedback and article_reply_feedback['user'] is not None:
                                                article_reply_feedback_dict[article_reply_feedback['id']].userId = article_reply_feedback['user'].get('id', None)
                                                article_reply_feedback_dict[article_reply_feedback['id']].user = article_reply_feedback['user'].get('name', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].comment = article_reply_feedback.get('comment', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].score = article_reply_feedback.get('score', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT']) 
                                            db.session.add(article_reply_feedback_dict[article_reply_feedback['id']])

                            if article_dict['node']['replyRequests']:
                                conds = [Reply_requests.replyRequestsId==reply_request['id'] for reply_request in article_dict['node']['replyRequests']]
                                reply_request_dict = {db_result.replyRequestsId:db_result for db_result in Reply_requests.query.filter(or_(*conds)).all()}
                                for reply_request in article_dict['node']['replyRequests']:
                                    if reply_request['id'] not in reply_request_dict:
                                        reply_request_dict[reply_request['id']] = Reply_requests()
                                        reply_request_dict[reply_request['id']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                        reply_request_dict[reply_request['id']].replyRequestsId = reply_request['id']
                                    reply_request_dict[reply_request['id']].articleId = reply_request['id'].split('__')[0]
                                    reply_request_dict[reply_request['id']].userId = reply_request.get('userId', None)
                                    reply_request_dict[reply_request['id']].appId = reply_request.get('appId', None)
                                    reply_request_dict[reply_request['id']].reason = reply_request.get('reason', None)
                                    reply_request_dict[reply_request['id']].negativeFeedbackCount = reply_request.get('negativeFeedbackCount', 0)
                                    reply_request_dict[reply_request['id']].positiveFeedbackCount = reply_request.get('positiveFeedbackCount', 0)
                                    reply_request_dict[reply_request['id']].feedbackCount = reply_request.get('feedbackCount', 0)
                                    reply_request_dict[reply_request['id']].createdAt = isodate_to_local(reply_request, 'createdAt')
                                    reply_request_dict[reply_request['id']].updatedAt = isodate_to_local(reply_request, 'updatedAt')
                                    reply_request_dict[reply_request['id']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT']) 
                                    db.session.add(reply_request_dict[reply_request['id']])

                            if article_dict['node']['hyperlinks']:
                                referenceId_list = ['{}{}{}'.format(article_dict['node']['id'], '.hyperlinks.', str(i)) for i, reference_data in enumerate(article_dict['node']['hyperlinks'])]
                                conds = [Reference.referenceId==key for key in referenceId_list]
                                reference_dict = {db_result.referenceId:db_result for db_result in Reference.query.filter(or_(*conds)).all()}
                                for i, reference_data in enumerate(article_dict['node']['hyperlinks']):
                                    if referenceId_list[i] not in reference_dict:
                                        reference_dict[referenceId_list[i]] = Reference()
                                        reference_dict[referenceId_list[i]].referenceId = referenceId_list[i]
                                        reference_dict[referenceId_list[i]].sourceId = article_dict['node']['id']
                                        reference_dict[referenceId_list[i]].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    reference_dict[referenceId_list[i]].url = reference_data.get('url', None)
                                    reference_dict[referenceId_list[i]].normalizedUrl = reference_data.get('normalizedUrl', None)
                                    reference_dict[referenceId_list[i]].title = reference_data.get('title', None)
                                    reference_dict[referenceId_list[i]].summary = reference_data.get('summary', None)
                                    reference_dict[referenceId_list[i]].topImageUrl = reference_data.get('topImageUrl', None)
                                    reference_dict[referenceId_list[i]].fetchedAt = isodate_to_local(reference_data, 'fetchedAt')
                                    reference_dict[referenceId_list[i]].status = reference_data.get('status', None)
                                    reference_dict[referenceId_list[i]].error = reference_data.get('error', None)
                                    reference_dict[referenceId_list[i]].updatetime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    db.session.add(reference_dict[referenceId_list[i]])
                        db.session.commit()

                    this_round_start_datetime = datetime.now()
                    this_rount_start_time = time.time()
                    rsp_result['data']['ListArticles']['edges'].reverse()
                    for article_index, article_dict in enumerate(rsp_result['data']['ListArticles']['edges']):
                        print('{}/{}, {}/{}, 本次開始時間: {}, 現在時間: {}'.format(article_index, len(rsp_result['data']['ListArticles']['edges']), isodate_to_local(article_dict['node'], 'lastRequestedAt'), createdAt_start, this_round_start_datetime, datetime.now()))
                        article_dict['node']['text_tc'] = convert_to_tc(article_dict['node']['text'])
                        article_dict['node']['replies'] = article_dict['node'].pop('articleReplies')
                        article_dict['node']['lastRequestedAt'] = isodate_to_local(article_dict['node'], 'lastRequestedAt') 
                        article_dict['node']['createdAt'] = isodate_to_local(article_dict['node'], 'createdAt')
                        article_dict['node']['updatedAt'] = isodate_to_local(article_dict['node'], 'updatedAt')
                        article_dict['node']['updatetime'] = datetime.now().strftime(app.config['TIMEFORMAT'])

                        # 有可能帶None，故要再一層if檢查一下
                        if type(article_dict['node']['hyperlinks']) is list:
                            for index_n, ddd in enumerate(article_dict['node']['hyperlinks']):
                                article_dict['node']['hyperlinks'][index_n]['fetchedAt'] = isodate_to_local(article_dict['node']['hyperlinks'][index_n], 'fetchedAt')
                        if type(article_dict['node']['replies']) is list:
                            for index_n, ddd in enumerate(article_dict['node']['replies']):
                                article_dict['node']['replies'][index_n]['createdAt'] = isodate_to_local(article_dict['node']['replies'][index_n], 'createdAt')
                                article_dict['node']['replies'][index_n]['updatedAt'] = isodate_to_local(article_dict['node']['replies'][index_n], 'updatedAt')
                        if type(article_dict['node']['replyRequests']) is list:
                            for index_n, ddd in enumerate(article_dict['node']['replyRequests']):
                                article_dict['node']['replyRequests'][index_n]['createdAt'] = isodate_to_local(article_dict['node']['replyRequests'][index_n], 'createdAt')
                                article_dict['node']['replyRequests'][index_n]['updatedAt'] = isodate_to_local(article_dict['node']['replyRequests'][index_n], 'updatedAt')

                        ##set grouping data structure
                        article_url = []
                        if article_dict['node']['hyperlinks']:
                            for url_info in article_dict['node']['hyperlinks']:
                                article_url.append(url_info['url']) 

                        es_result = es.search_by_id(app.config['ARTICLE_INDEX'], '_doc', article_dict['node']['id'], routing=None)
                        batch_load_list = []
                        if es_result['found'] and es_result['_source'].get('rg_status')==True:
                            rg_es_result = es.search_by_id(app.config['GROUPING_INDEX'], '_doc', es_result['_source']['replyRequests'][0]['rumor_id'], routing=None)
                        else:
                            rg_es_result = {
                                'found':False
                            }
                        if es_result['found'] and es_result['_source'].get('rg_status')==True and rg_es_result['found']:
                            rg_es_result = es.search_by_id(app.config['GROUPING_INDEX'], '_doc', es_result['_source']['replyRequests'][0]['rumor_id'], routing=None)
                            es_replyRequest_dict = {x['id']:x['rumor_id'] for x in es_result['_source']['replyRequests']}
                            for replyRequest_dict_index, replyRequest_dict in enumerate(article_dict['node']['replyRequests']):
                                if replyRequest_dict['id'] in es_replyRequest_dict:
                                    article_dict['node']['replyRequests'][replyRequest_dict_index]['rumor_id'] = es_replyRequest_dict[replyRequest_dict['id']]
                                    continue
                                dictionary = {
                                    '_id':uuid.uuid3(uuid.NAMESPACE_DNS, '{}_{}_{}_{}'.format('cofacts', 'web', article_dict['node']['id'], replyRequest_dict['id'])),
                                    '_index': app.config['GROUPING_INDEX'],
                                    'article_id':article_dict['node']['id'],
                                    'language':rg_es_result['_source']['language'],
                                    'language_rate':rg_es_result['_source']['language_rate'],
                                    'group_id': rg_es_result['_source']['group_id'],
                                    'source': 'cofacts',
                                    'platform':'web',
                                    'type': rg_es_result['_source']['type'],
                                    'message': article_dict['node']['text'],
                                    'message_tc': article_dict['node']['text_tc'],
                                    'link': rg_es_result['_source']['link'],
                                    'link_message': rg_es_result['_source']['link_message'],
                                    'file': [],
                                    'file_name': [],
                                    'create_time': replyRequest_dict['createdAt'] if replyRequest_dict['createdAt'] else article_dict['node']['createdAt'],
                                    'db_update_time': datetime.now().strftime(app.config['TIMEFORMAT']) 
                                }
                                article_dict['node']['replyRequests'][replyRequest_dict_index]['rumor_id'] = deepcopy(dictionary['_id'])
                                batch_load_list.append(dictionary)
                        elif es_result['found'] and es_result['_source'].get('rg_status')==False:
                            continue
                        else:
                            get_group_id_json = {
                                'class_name': 'CofactsGrouping',
                                'original_text':article_dict['node']['text'],
                                'text':article_dict['node']['text_tc'].replace(u'\ufeff', ''),
                                'url': article_url
                            }
                            err_msg_list = []
                            rg_retry_n = 0
                            while rg_retry_n<=app.config['RETRY_LIMIT']:
                                rg_retry_n+=1
                                try:
                                    rsp = requests.post(app.config['RUMOR_GROUPING_API_URL'], json=get_group_id_json, timeout=600)
                                    if rsp.status_code!=200:
                                        err_text = '文章 {} 介接 Grouping API 出現錯誤, 錯誤代碼: {}'.format(article_dict['node']['id'], rsp.status_code)
                                        logging.error(err_text)
                                        if err_text not in err_msg_list:
                                            err_msg_list.append(err_text)
                                        continue
                                    
                                    rg_rsp_result = rsp.json()
                                    if not rg_rsp_result['status']:
                                        if rg_rsp_result['message'] not in err_msg_list:
                                            err_msg_list.append(rg_rsp_result['message'])
                                        logging.error(rg_rsp_result['message'])
                                        continue
                                    break
                                except:
                                    err_msg = traceback.format_exc()
                                    logging.error(err_msg)
                                    if err_text not in err_msg_list:
                                        err_msg_list.append(err_text)
                                    time.sleep(3)
                                    continue
                            if rg_retry_n>=app.config['RETRY_LIMIT']:
                                raise(Exception('\n'.join(err_msg_list)))

                            if rg_rsp_result['result'].get('group_id'):
                                article_dict['node']['rg_status'] = True
                                for replyRequest_dict_index, replyRequest_dict in enumerate(article_dict['node']['replyRequests']):
                                    article_url_content = rg_rsp_result['result']['url_content']
                                    article_type = []
                                    if article_dict['node']['text']:
                                        article_type.append('text')
                                    if article_dict['node']['hyperlinks']:
                                        article_type.append('link')
                                    dictionary = {
                                        '_id':'{}'.format(uuid.uuid3(uuid.NAMESPACE_DNS, '{}_{}_{}_{}'.format('cofacts', 'web', article_dict['node']['id'], replyRequest_dict['id']))),
                                        '_index': app.config['GROUPING_INDEX'],
                                        'article_id':article_dict['node']['id'],
                                        'language':rg_rsp_result['result']['language'],
                                        'language_rate':rg_rsp_result['result']['language_rate'],
                                        'group_id': rg_rsp_result['result']['group_id'],
                                        'source': 'cofacts',
                                        'platform':'web',
                                        'type': article_type,
                                        'message': article_dict['node']['text'],
                                        'message_tc': article_dict['node']['text_tc'],
                                        'link': list(article_url_content.keys()),
                                        'link_message': list(article_url_content.values()),
                                        'file': [],
                                        'file_name': [],
                                        'create_time': replyRequest_dict['createdAt'] if replyRequest_dict['createdAt'] else article_dict['node']['createdAt'],
                                        'db_update_time': datetime.now().strftime(app.config['TIMEFORMAT']) 
                                    }
                                    batch_load_list.append(dictionary)
                                    article_dict['node']['replyRequests'][replyRequest_dict_index]['rumor_id'] = deepcopy(dictionary['_id'])
                            else:
                                article_dict['node']['rg_status'] = False
                        es_data = {
                            '_index':app.config['ARTICLE_INDEX'],
                            '_id':article_dict['node'].pop('id')
                        }
                        es_data.update(article_dict['node'])
                        batch_load_list.append(es_data)
                        es.batch_load(batch_load_list)

                    print('This round cost: {} second(s).'.format(time.time()-this_rount_start_time))
                    break
                except Exception as e:
                    err_msg_list.append(traceback.format_exc())
                    logger.error('Sync data error')
                    logger.error(e)
                    traceback.print_exc()
            if retry_n>=app.config['RETRY_LIMIT']:
                raise Exception('\n'.join(list(set(err_msg_list))))
        logger.info('End sync article data')
    except:
        err_msg = traceback.format_exc()
        logging.error(err_msg)
        lnm.send_msg('{}: {}'.format('Cofacts Article 排程出現錯誤', err_msg))
        gs = GmailSender('Cofacts Article 排程出現錯誤', app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'], err_msg)
        gs.send_email()

def sync_reply_list():
    try:
        script_name = sys.argv[1] if len(sys.argv)>1 else 'reply'
        if check_duplicate_process(script_name):
            print(f'{script_name} :尚有相同的程式正在執行')
            return
        logfile_path = './log/[reply]%s.log' % datetime.today().strftime('%Y-%m-%d')
        logger = new_logger('cofacts_reply', logfile_path)
        es = Elastic(**app.config['FAKENEWS_DATA_ESHOST'])

        if not es.check_index_exist(app.config['REPLY_INDEX']):
            es.create_index(app.config['REPLY_INDEX'], app.config['REPLY_MAPPING_PATH'])

        retry_limit = 5
        logger.info('Start sync reply data')

        err_msg_list = []
        finish_status = False
        createdAt_start = None
        while not finish_status:
            if not createdAt_start:
                # createdAt_start = (datetime.now()-timedelta(days=7))
                # createdAt_start = datetime.strptime('2018-10-13 13:29:14', '%Y-%m-%d %H:%M:%S')
                # createdAt_start = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                query = {
                    'from' : 0, 
                    'size' : 1, 
                    "sort":[
                        {
                            "createdAt":{
                                "order":"desc"
                            }
                        }
                    ],
                    "query":{
                        "bool":{
                            "must":[]
                        }
                    }
                }
                es_result = es.search(query, app.config['REPLY_INDEX'])
                if not es_result['hits']['hits']:
                    createdAt_start = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                else:
                    # 因為Cofacts的API createdAt時區是格林威治時間，故要再減8小時
                    if (datetime.strptime(es_result['hits']['hits'][0]['_source']['createdAt'], '%Y-%m-%d %H:%M:%S')-timedelta(hours=8))<(datetime.now()-timedelta(days=7, hours=8)):
                        createdAt_start = (datetime.strptime(es_result['hits']['hits'][0]['_source']['createdAt'], '%Y-%m-%d %H:%M:%S')-timedelta(hours=8))
                    else:
                        createdAt_start = (datetime.now()-timedelta(days=7, hours=8))
            print(createdAt_start)
            query = deepcopy(app.config['REPLY_QUERY_JSON'])
            if 'filter' not in query['variables']:
                query['variables']['filter'] = {}
            query['variables']['filter']['createdAt'] = {'GT':createdAt_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
            retry_n = 0
            while retry_n <= retry_limit:
                retry_n+=1
                try:
                    db.session.rollback()
                    db.session.close()
                    rsp = requests.post(app.config['COFACTS_GRAPHQL_URL'], json=query)
                    rsp.encoding='utf-8'
                    if rsp.status_code!=200:
                        logger.error('Request API error')
                        logger.error(rsp.text)
                        time.sleep(5)
                        continue
                    rsp_result = rsp.json()
                    if not rsp_result['data']['ListReplies']['edges']:
                        finish_status = True
                        break

                    if not rsp_result['data']['ListReplies']['edges']:
                        finish_status = True
                        break
                    createdAt_start = datetime.strptime(rsp_result['data']['ListReplies']['edges'][-1]['node']['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ')

                    if os.environ['API_PROPERTY']=='FORMALITY':
                        conds = [Reply.replyId==reply_data['node']['id'] for reply_data in rsp_result['data']['ListReplies']['edges']]
                        db_result_dict = {db_result.replyId:db_result for db_result in Reply.query.filter(or_(*conds)).all()}
                        for reply_data in rsp_result['data']['ListReplies']['edges']:
                            if reply_data['node']['id'] not in db_result_dict:
                                db_result_dict[reply_data['node']['id']] = Reply()
                                db_result_dict[reply_data['node']['id']].createtime = isodate_to_local(reply_data['node'], 'createtime')
                                db_result_dict[reply_data['node']['id']].replyId = reply_data['node'].get('id', None)
                            db_result_dict[reply_data['node']['id']].text = reply_data['node'].get('text', None)
                            db_result_dict[reply_data['node']['id']].type = reply_data['node'].get('type', None)  
                            db_result_dict[reply_data['node']['id']].type_code = reply_type_code[reply_data['node']['type']]

                            if 'user' in reply_data['node'] and reply_data['node']['user']:
                                db_result_dict[reply_data['node']['id']].userId = reply_data['node']['user'].get('id', None)
                                db_result_dict[reply_data['node']['id']].user = reply_data['node']['user'].get('name', None)
                            db_result_dict[reply_data['node']['id']].createdAt = isodate_to_local(reply_data['node'],'createdAt')
                            db_result_dict[reply_data['node']['id']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT'])
                            db.session.add(db_result_dict[reply_data['node']['id']])

                            if reply_data['node']['articleReplies']:
                                conds = []
                                for article_reply in reply_data['node']['articleReplies']:
                                    conds.append(and_(Article_reply.articleId==article_reply['articleId'], Article_reply.replyId==article_reply['replyId']))
                                article_reply_dict = {}
                                for db_result in Article_reply.query.filter(or_(*conds)).all():
                                    if db_result.articleId not in article_reply_dict:
                                        article_reply_dict[db_result.articleId] = {}
                                    article_reply_dict[db_result.articleId][db_result.replyId] = db_result
                                for article_reply in reply_data['node']['articleReplies']:
                                    if article_reply['articleId'] not in article_reply_dict:
                                        article_reply_dict[article_reply['articleId']] = {}
                                    if article_reply['replyId'] not in article_reply_dict[article_reply['articleId']]:
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']] = Article_reply()
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']].articleId = article_reply.get('articleId', None)
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']].replyId = article_reply.get('replyId', None)
                                    if 'user' in article_reply and article_reply['user']:
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']].userId = article_reply['user'].get('id', None)
                                        article_reply_dict[article_reply['articleId']][article_reply['replyId']].user = article_reply['user'].get('name', None)
                                    
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].negativeFeedbackCount = article_reply.get('negativeFeedbackCount', 0)
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].positiveFeedbackCount =article_reply.get('positiveFeedbackCount', 0)
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].status = article_reply.get('status', None)
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].canUpdateStatus = article_reply.get('canUpdateStatus', None)
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].createdAt = isodate_to_local(article_reply, 'createdAt')
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].updatedAt = isodate_to_local(article_reply, 'updatedAt')
                                    article_reply_dict[article_reply['articleId']][article_reply['replyId']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    db.session.add(article_reply_dict[article_reply['articleId']][article_reply['replyId']])

                                    if article_reply['feedbacks']:
                                        conds = [Article_reply_feedback.articleReplyFeedbackId==article_reply_feedback['id'] for article_reply_feedback in article_reply['feedbacks']]
                                        article_reply_feedback_dict = {db_result.articleReplyFeedbackId:db_result for db_result in Article_reply_feedback.query.filter(or_(*conds)).all()}
                                        for article_reply_feedback in article_reply['feedbacks']:
                                            if article_reply_feedback['id'] not in article_reply_feedback_dict:
                                                article_reply_feedback_dict[article_reply_feedback['id']] = Article_reply_feedback()
                                                article_reply_feedback_dict[article_reply_feedback['id']].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                                article_reply_feedback_dict[article_reply_feedback['id']].articleReplyFeedbackId = article_reply_feedback['id']
                                            article_reply_feedback_dict[article_reply_feedback['id']].articleId = article_reply_feedback['id'].split('__')[0]
                                            article_reply_feedback_dict[article_reply_feedback['id']].replyId = article_reply_feedback['id'].split('__')[1]
                                            if 'user' in article_reply_feedback and article_reply_feedback['user'] is not None:
                                                article_reply_feedback_dict[article_reply_feedback['id']].userId = article_reply_feedback['user'].get('id', None)
                                                article_reply_feedback_dict[article_reply_feedback['id']].user = article_reply_feedback['user'].get('name', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].comment = article_reply_feedback.get('comment', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].score = article_reply_feedback.get('score', None)
                                            article_reply_feedback_dict[article_reply_feedback['id']].updatetime = datetime.now().strftime(app.config['TIMEFORMAT']) 
                                            db.session.add(article_reply_feedback_dict[article_reply_feedback['id']])

                            if reply_data['node']['hyperlinks']:
                                referenceId_list = ['{}{}{}'.format(reply_data['node']['id'], '.hyperlinks.', str(i)) for i, reference_data in enumerate(reply_data['node']['hyperlinks'])]
                                conds = [Reference.referenceId==key for key in referenceId_list]
                                reference_dict = {db_result.referenceId:db_result for db_result in Reference.query.filter(or_(*conds)).all()}
                                for i, reference_data in enumerate(reply_data['node']['hyperlinks']):
                                    if referenceId_list[i] not in reference_dict:
                                        reference_dict[referenceId_list[i]] = Reference()
                                        reference_dict[referenceId_list[i]].referenceId = referenceId_list[i]
                                        reference_dict[referenceId_list[i]].sourceId = reply_data['node']['id']
                                        reference_dict[referenceId_list[i]].createtime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    reference_dict[referenceId_list[i]].url = reference_data.get('url', None)
                                    reference_dict[referenceId_list[i]].normalizedUrl = reference_data.get('normalizedUrl', None)
                                    reference_dict[referenceId_list[i]].title = reference_data.get('title', None)
                                    reference_dict[referenceId_list[i]].summary = reference_data.get('summary', None)
                                    reference_dict[referenceId_list[i]].topImageUrl = reference_data.get('topImageUrl', None)
                                    reference_dict[referenceId_list[i]].fetchedAt = isodate_to_local(reference_data, 'fetchedAt')
                                    reference_dict[referenceId_list[i]].status = reference_data.get('status', None)
                                    reference_dict[referenceId_list[i]].error = reference_data.get('error', None)
                                    reference_dict[referenceId_list[i]].updatetime = datetime.now().strftime(app.config['TIMEFORMAT'])
                                    db.session.add(reference_dict[referenceId_list[i]])

                        db.session.commit()

                    batch_load_list = []
                    for reply_data in rsp_result['data']['ListReplies']['edges']:
                        if reply_data['node']['hyperlinks']:
                            for iii in range(0, len(reply_data['node']['hyperlinks'])):
                                reply_data['node']['hyperlinks'][iii]['fetchedAt'] = isodate_to_local(reply_data['node']['hyperlinks'][iii], 'fetchedAt')  

                        reply_data['node']['type_code'] = reply_type_code[reply_data['node']['type']]
                        reply_data['node'].pop('articleReplies')
                        reply_data['node']['createdAt'] = isodate_to_local(reply_data['node'], 'createdAt')
                        reply_data['node']['updatetime'] = datetime.now().strftime(app.config['TIMEFORMAT'])  
                        es_data = {
                            '_index':app.config['REPLY_INDEX'],
                            '_type':'_doc',
                            '_id':reply_data['node'].pop('id')
                        }
                        es_data.update(reply_data['node'])
                        batch_load_list.append(es_data)
                    es.batch_load(batch_load_list)
                    break
                except Exception as e:
                    err_msg_list.append(traceback.format_exc())
                    logger.error('Sync data error')
                    logger.error(e)
                    traceback.print_exc()
            if retry_n>=retry_limit:
                raise Exception('\n'.join(list(set(err_msg_list))))
        logger.info('Finish sync %d data' % (app.config['QUERY_SIZE']))
    except:
        err_msg = traceback.format_exc()
        logging.error(err_msg)
        lnm.send_msg('{}: {}'.format('Cofacts Reply 排程出現錯誤', err_msg))
        gs = GmailSender('Cofacts Reply 排程出現錯誤', app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'], err_msg)
        gs.send_email()

if __name__ == '__main__':
    pass
    #get_article_list()
    #get_reply_list()