if __name__ == '__main__':
    import sys
    sys.path.append('/app/')
from sqlalchemy           import Column, String, SmallInteger
from settings.environment import db

class ShipTypeShipxy(db.Model):
    __tablename__ = 'ship_type_shipxy'
    type          = Column('type'   , SmallInteger, primary_key=True)
    name          = Column('name_tc', String)

class ShipTypeMyships(db.Model):
    __tablename__ = 'ship_type_myships'
    type          = Column('type'   , SmallInteger, primary_key=True)
    name          = Column('name_tc', String)

class NavistatusTypeShipxy(db.Model):
    __tablename__ = 'navistatus_type_shipxy'
    type          = Column('type'   , SmallInteger, primary_key=True)
    name          = Column('name_tc', String)

class NavistatusTypeMyships(db.Model):
    __tablename__ = 'navistatus_type_myships'
    type          = Column('type'   , SmallInteger, primary_key=True)
    name          = Column('name_tc', String)