from uuid import uuid1
from datetime             import datetime
from sqlalchemy           import ForeignKey, Column, String, DateTime, Text, text, Float
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm       import relationship
from settings.environment import db

class SubAreaList(db.Model):
    __tablename__ = 'sub_area_list'
    id            = Column('id',           String(36), primary_key=True)
    area_list_id  = Column('area_list_id', String(36), ForeignKey('area_list.id', ondelete='CASCADE'))
    web           = Column('web',          String(10))
    lu_lat        = Column('lu_lat',       Float)
    lu_lng        = Column('lu_lng',       Float)
    rd_lat        = Column('rd_lat',       Float)
    rd_lng        = Column('rd_lng',       Float)
    enable        = Column('enable',       TINYINT)
    crawler_time  = Column('crawler_time', DateTime)
    next_time     = Column('next_time',    DateTime)
    created_time  = Column('created_time', DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_time  = Column('updated_time', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    area_list = relationship('AreaList', backref=__tablename__)

    def __init__(self, area_list_id, web, lu_lat, lu_lng, rd_lat, rd_lng, enable, *args, **kwargs):
        self.id           = '{}'.format(uuid1())
        self.area_list_id = area_list_id
        self.web          = web
        self.lu_lat       = lu_lat
        self.lu_lng       = lu_lng
        self.rd_lat       = rd_lat
        self.rd_lng       = rd_lng
        self.enable       = enable

    def json(self):
        return(
            {
                'id':self.id,
                'area_list_id':self.area_list_id,
                'web'   :self.web,
                'lu_lat':self.lu_lat,
                'lu_lng':self.lu_lng,
                'rd_lat':self.rd_lat,
                'rd_lng':self.rd_lng,
                'enable':self.enable,
                'crawler_time':self.crawler_time.strftime('%Y-%m-%d %H:%M:%S') if self.crawler_time else None,
                'next_time':self.next_time.strftime('%Y-%m-%d %H:%M:%S') if self.next_time else None,
                'created_time' : self.created_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time else None,
                'updated_time' : self.updated_time.strftime('%Y-%m-%d %H:%M:%S') if self.updated_time else None
            }
        )