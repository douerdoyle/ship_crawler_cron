from uuid import uuid1
from datetime             import datetime
from sqlalchemy           import Column, String, DateTime, Text, text, SmallInteger, Float
from settings.environment import db
from sqlalchemy.dialects.mysql import TINYINT

class AreaList(db.Model):
    __tablename__ = 'area_list'
    id            = Column('id',           String(36), primary_key=True)
    serial        = Column('serial',       SmallInteger)
    name          = Column('name',         Text)
    # type          = Column('type',         SmallInteger)
    description   = Column('description',  Text)
    lu_lat        = Column('lu_lat',       Float)
    lu_lng        = Column('lu_lng',       Float)
    rd_lat        = Column('rd_lat',       Float)
    rd_lng        = Column('rd_lng',       Float)
    split_status  = Column('split_status', TINYINT)
    enable        = Column('enable',       TINYINT)
    crawl_span    = Column('crawl_span',   SmallInteger)
    created_time  = Column('created_time', DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_time  = Column('updated_time', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def __init__(self, name, serial, lu_lat, lu_lng, rd_lat, rd_lng, description=None, split_status=0, enable=1, crawl_span=30, *args, **kwargs):
        self.id           = '{}'.format(uuid1())
        self.name         = name
        self.serial       = serial
        self.lu_lat       = lu_lat
        self.lu_lng       = lu_lng
        self.rd_lat       = rd_lat
        self.rd_lng       = rd_lng
        self.description  = description
        self.split_status = split_status
        self.enable       = enable
        self.crawl_span   = crawl_span
        self.description  = description
