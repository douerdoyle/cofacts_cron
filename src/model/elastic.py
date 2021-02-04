#coding:utf-8
import os
import json
from uuid import uuid1
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def query_parser(query_str):
    query_str = ' %s ' % query_str.replace('AND', ' AND ').replace('OR', ' OR ').replace('NOT', ' NOT ').replace('(', '( ').replace(')', ' )')  
    keyword_str = query_str.replace('AND', '').replace('OR', '').replace('NOT', '').replace('(', '').replace(')', '')  
    keyword_list = keyword_str.split()
    for keyword in keyword_list:
        query_str = query_str.replace(' %s ' % keyword, ' \"%s\" ' % keyword)
    return query_str

def load_settings(path):
    with open(path, 'r', encoding='utf-8') as st:
        settings = json.load(st)
    return settings

def load_mappings(path):
    with open(path, 'r', encoding='utf-8') as mp:
        mappings = json.load(mp)
    return mappings

class Elastic(object):
    def __init__(self, host=[], username='', password='', timeout=480, max_retries=10, retry_on_timeout=True):
        try:
            self.es = Elasticsearch(hosts=host, http_auth=(username, password), timeout=timeout, max_retries=max_retries, retry_on_timeout=retry_on_timeout)
        except Exception as err:
            print (err)

    def create_index(self, index_name, mapping_path):
        mapping = load_mappings(mapping_path)
        return self.es.indices.create(index=index_name, body=mapping, ignore=400)

    def delete_index(self, index_name):
        return self.es.indices.delete(index=index_name) 

    def refresh_index(self, index_name):
        self.es.indices.refresh(index=index_name)   

    def check_index_exist(self, index_name):
        return True if self.es.indices.exists(index_name) else False

    def count(self, query, index_name):
        return self.es.count(index=index_name, body=query)['count']
        
    def genEs_data(self, datas):
        es_data=[]
        for data in datas:
            type_name = data.pop('_type', None)
            _format= {
                '_index'  : data.pop('_index'),
                '_id'     : data.pop('_id'),
                '_routing': data.pop('_routing', None),
                'doc_as_upsert': 'true',
                '_source' : {key: value for key, value in data.items()} 
            }
            if type_name: _format['_type'] = type_name
            es_data.append(_format)
        return es_data

    def batch_load(self, datas):
        es_data = self.genEs_data(datas)
        helpers.bulk(self.es, es_data, refresh='wait_for')

    def update_data(self, datas):
        for data in datas:
            _format= {
                '_op_type': 'update',
                '_index'  : data.pop('_index'),
                '_id'     : data.pop('_id'),
                'doc'     :{key: value for key, value in data.items()} 
            }
            type_name = data.pop('_type', None)
            if type_name: _format['_type'] = type_name
            helpers.bulk(self.es, [_format])

    def delete_data(self, datas):
        for data in datas:
            _format= {
                '_op_type': 'delete',
                '_index'  : data.pop('_index'),
                '_id'     : data.pop('_id')
            }
            type_name = data.pop('_type', None)
            if type_name: _format['_type'] = type_name
            helpers.bulk(self.es, [_format])

    def scan(self, query, index_name):
        return helpers.scan(self.es, query=query, index=index_name)

    def search(self, query, index_name):
        return self.es.search(index=index_name, body=query)

    def search_by_id(self, index_name, type_name, _id, routing=None):
        return self.es.get(index=index_name, doc_type=type_name, id=_id, routing=routing, ignore=404)

    def validate_query(self, body, index_name, explain=False, rewrite=False):
        return self.es.indices.validate_query(body=body, index=index_name, explain=explain, rewrite=rewrite)


class ESQuery(object):
    def __init__(self, sort_field=None, sort_order='desc', size=None, page=None):
        self.es_query = {'query':{'bool':{}}}
        self.set_sort(sort_field, sort_order)
        self.set_paging(page, size) 
   
    def checkQuery(self, query, field, type=None):
        if field not in query:
            query[field] = []

    def set_timeRange(self, date_from, date_to, date_field):
        time_range = {}
        time_range['gt'] = date_from if date_from else None
        time_range['lt'] = date_to if date_to else None

        filter_range = {'range':{date_field: time_range}}

        if date_from or date_to:
            self.checkQuery(self.es_query['query']['bool'], 'must')
            self.es_query['query']['bool']['must'].append(filter_range)

    def set_range(self, range_field, gt=None, gte=None, lt=None, lte=None):
        range_filter = {}
        if gt is not None: 
            range_filter['gt'] = gt
        if gte is not None: 
            range_filter['gte'] = gte
        if lt is not None: 
            range_filter['lt'] = lt
        if lte is not None: 
            range_filter['lte'] = lte

        range_query = {'range':{range_field: range_filter}}
        self.checkQuery(self.es_query['query']['bool'], 'must')
        self.es_query['query']['bool']['must'].append(range_query)

    def set_terms(self, terms, term_field):
        self.checkQuery(self.es_query['query']['bool'], 'must')
        self.es_query['query']['bool']['must'].append({'terms': {term_field: terms}})

    def set_match(self, text, field, analyzer, nested_path=None):
        self.checkQuery(self.es_query['query']['bool'], 'must')
        match_query_term = {
            'multi_match':{
                'query':text,
                'fields':field,
                'analyzer':analyzer
            }
        }
        if nested_path:
            query_term = {
                'nested': {
                    'path': nested_path,
                    'query':match_query_term
                }
            }
        else:
            query_term = match_query_term
        self.es_query['query']['bool']['must'].append(query_term)

    def set_keyword(self, keyword, keyword_field, nested_path=None):
        if keyword:
            query_term = query_parser(keyword)
            self.checkQuery(self.es_query['query']['bool'], 'must')
            qs_query_term = {
                'query_string':{
                    'query': query_term,
                    'fields': keyword_field
                }
            }
            if nested_path:
                query_term = {
                    'nested': {
                        'path': nested_path,
                        'query':qs_query_term
                    }
                }
            else:
                query_term = qs_query_term
            self.es_query['query']['bool']['must'].append(query_term)

    def add_keyword(self, keyword, keyword_field):
        query_term = query_parser(keyword)
        self.checkQuery(self.es_query['query']['bool'], 'filter')
        self.es_query['query']['bool']['filter'].append({'query_string':{'query': query_term,'fields': keyword_field}})

    def set_aggsType(self, agg_field):
        if agg_field:
            self.es_query['aggs'] = {agg_field:{'terms': {'field': agg_field}}}

    def set_field_exist(self, field, exist=True):
        if exist:
            self.checkQuery(self.es_query['query']['bool'], 'must')
            self.es_query['query']['bool']['must'].append({'nested': {'path': field, 'query':{'exists':{'field':field}}}})
        elif not exist:
            self.checkQuery(self.es_query['query']['bool'], 'must_not')
            self.es_query['query']['bool']['must_not'].append({'nested': {'path': field, 'query':{'exists':{'field':field}}}})

    def add_explain(self):
        self.es_query['explain'] = 'true'

    def set_sort(self, sort_field, sort_order):
        if sort_field:
            self.sort = []
            self.sort.append({sort_field: {"order": sort_order}})
            self.es_query['sort'] = self.sort

    def set_paging(self, page, size):
        if size:
            self.es_query['size'] = size
        if page:
            self.es_query['from'] = page * size

    def add_highlight(self, highlight_field, color='red', font_weight='bold'):
        self.es_query['highlight'] = {
            'tags_schema':'styled',
            'pre_tags':['<span style=\"color:%s; font-weight:%s\">' % (color, font_weight)],
            'post_tags':['</span>'],
            'fields':{}
        }
        self.es_query['highlight']['fields'] = {field:{'number_of_fragments':0} for field in highlight_field}

    def more_like_this(self, fields=[], like_text='', max_query_terms=25, min_term_freq=1, min_doc_freq=5, min_word_length=0, analyzer='', nested_path=None):
        self.checkQuery(self.es_query['query']['bool'], 'must')
        mlt_query_term = {
          'more_like_this': {
            'fields': fields,
            'like': like_text,
            'max_query_terms': max_query_terms,
            'min_term_freq': min_term_freq,
            'min_doc_freq': min_doc_freq,
            'min_word_length':min_word_length,
            'analyzer': analyzer
          }           
        }
        if nested_path:
            query_term = {
                'nested': {
                    'path': nested_path,
                    'query':mlt_query_term
                }
            }
        else:
            query_term = mlt_query_term

        self.es_query['query']['bool']['must'].append(query_term)

    
