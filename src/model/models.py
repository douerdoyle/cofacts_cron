from datetime import datetime
from sqlalchemy.databases import mysql
from sqlalchemy import Column, Integer, String, Text, SmallInteger, LargeBinary, Date, DateTime, ForeignKey
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT
from sqlalchemy.orm import relationship
from setting.settings import app, db

REGISTER_TABLES = {}

    
class Article(db.Model):
    __tablename__ = 'article_list'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }
    articleId = Column('articleId', String(255), primary_key=True)
    text = Column('text', Text, default=None)
    replyRequestCount = Column('replyRequestCount', Integer, default=0)  
    replyCount = Column('replyCount', Integer, default=0)
    lastRequestedAt = Column('lastRequestedAt', DateTime, default=None)
    createdAt = Column('createdAt', DateTime, default=None)
    updatedAt = Column('updatedAt', DateTime, default=None)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now())

class Reply(db.Model):
    __tablename__ = 'reply_list'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }
    replyId = Column('replyId', String(255), primary_key=True)
    text = Column('text', Text, default=None)
    type = Column('type',String(255), default=None)
    type_code = Column('type_code',Integer, default=0)
    reference = Column('reference', Text, default=None)
    userId = Column('userId', String(255), default=None)
    user = Column('user', String(255), default=None)    
    createdAt = Column('createdAt', DateTime, default=None)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now())
    



class Reply_requests(db.Model):
    __tablename__ = 'reply_requests'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }
    replyRequestsId = Column('replyRequestsId', String(255), primary_key=True) 
    articleId = Column('articleId', String(255))
    userId = Column('userId', String(255))
    appId = Column('appId', String(255))
    reason = Column('reason', Text, default=None)
    negativeFeedbackCount = Column('negativeFeedbackCount', Integer, default=0) 
    positiveFeedbackCount = Column('positiveFeedbackCount', Integer, default=0)
    feedbackCount = Column('feedbackCount', Integer, default=0)
    createdAt = Column('createdAt', DateTime, default=None)
    updatedAt = Column('updatedAt', DateTime, default=None)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now()) 
        
class Reference(db.Model):
    __tablename__ = 'reference'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }

    referenceId = Column('referenceId', String(255), primary_key=True)
    sourceId = Column('sourceId', String(255), default=None)
    title = Column('title', Text, default=None)
    url = Column('url', Text, default=None)
    summary = Column('summary', Text, default=None)
    topImageUrl = Column('topImageUrl', Text, default=None)
    normalizedUrl = Column('normalizedUrl', Text, default=None)
    status = Column('status',String(255), default=None)
    error = Column('error',String(255), default=None)
    fetchedAt = Column('fetchedAt', DateTime, default=None)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now())

class Article_reply(db.Model):
    __tablename__ = 'article_replies'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }
    articleId = Column('articleId', String(255), default=None, primary_key=True)
    replyId = Column('replyId', String(255), default=None, primary_key=True)
    userId = Column('userId', String(255), default=None)
    user = Column('user', String(255), default=None)
    negativeFeedbackCount = Column('negativeFeedbackCount', Integer, default=0) 
    positiveFeedbackCount = Column('positiveFeedbackCount', Integer, default=0)
    status = Column('status', String(255), default=None)
    canUpdateStatus = Column('canUpdateStatus', String(255), default=None)
    createdAt = Column('createdAt', DateTime, default=None)
    updatedAt = Column('updatedAt', DateTime, default=None)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now())

class Article_reply_feedback(db.Model):
    __tablename__ = 'article_reply_feedbacks'
    __table_args__ = {
        'useexisting': True,
        'mysql_charset': 'utf8mb4'
    }
    articleReplyFeedbackId = Column('articleReplyFeedbackId', String(255), primary_key=True)
    articleId = Column('articleId', String(255), default=None)
    replyId = Column('replyId', String(255), default=None)
    user = Column('user', String(255), default=None)
    userId = Column('userId', String(255), default=None)
    comment = Column('comment', Text, default=None)
    score = Column('score', Integer, default=0)
    createtime = Column('createtime', DateTime, default=datetime.now())
    updatetime = Column('updatetime', DateTime, default=datetime.now())
    
def create_table(model):
    try:
        print('create table '+model.__tablename__)
        model.__table__.create(db.get_engine(app))#model.__bind_key__
    except Exception as error_msg:
        #traceback.print_exc()
        print(error_msg)