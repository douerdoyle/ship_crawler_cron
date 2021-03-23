if __name__ == '__main__':
    import sys
    sys.path.append('../')
from uuid import uuid1
from datetime             import datetime
from sqlalchemy           import Column, String, DateTime, text, JSON
from settings.environment import db
from sqlalchemy.dialects.mysql import TINYINT

class ShipxyAccount(db.Model):
    __tablename__ = 'shipxy_account'
    account       = Column('account',  String(16), primary_key=True)
    password      = Column('password', String(16))
    cookies       = Column('cookies',  JSON)
    enable        = Column('enable',   TINYINT)
    updating      = Column('updating', TINYINT)
    created_time  = Column('created_time', DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_time  = Column('updated_time', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def __init__(self, account, password, cookies={}, enable=None, updating=0, *args, **kwargs):
        self.account  = account
        self.password = password
        self.cookies  = cookies
        self.enable   = enable
        self.updating = updating
    
    def json(self):
        return(
            {
                'account':self.account,
                'password':self.password,
                'enable':self.enable,
                'updating':self.updating
            }
        )

class MyshipsAccount(db.Model):
    __tablename__ = 'myships_account'
    account       = Column('account',  String(16), primary_key=True)
    password      = Column('password', String(16))
    cookies       = Column('cookies',  JSON)
    enable        = Column('enable',   TINYINT)
    updating      = Column('updating', TINYINT)
    created_time  = Column('created_time', DateTime, server_default=text('CURRENT_TIMESTAMP'))
    updated_time  = Column('updated_time', DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    def __init__(self, account, password, cookies={}, enable=None, updating=0, *args, **kwargs):
        self.account  = account
        self.password = password
        self.cookies  = cookies
        self.enable   = enable
        self.updating = updating

    def json(self):
        return(
            {
                'account':self.account,
                'password':self.password,
                'enable':self.enable,
                'updating':self.updating
            }
        )