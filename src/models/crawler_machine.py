from uuid import uuid1
from datetime             import datetime
from sqlalchemy           import Column, String, DateTime, Text, text, JSON, SmallInteger
from settings.environment import db
from sqlalchemy.dialects.mysql import TINYINT

class CrawlerMachine(db.Model):
    __tablename__ = 'crawler_machine'
    ip            = Column('ip',        String(45), primary_key=True)
    createdAt     = Column('createdAt', DateTime,   server_default=text('CURRENT_TIMESTAMP'))
    updatedAt     = Column('updatedAt', DateTime,   server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def __init__(self, ip):
        self.ip = ip