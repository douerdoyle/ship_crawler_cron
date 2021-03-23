import os, traceback, pymysql
pymysql.install_as_MySQLdb()
from flask             import Flask
from sqlalchemy_utils  import create_database, database_exists
from flask_sqlalchemy  import SQLAlchemy

app  = Flask(__name__)

class Config(object):
    DEBUG      = False
    TESTING    = False
    SECRET_KEY = ''
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    RUN_SETTING = {
        'host': '0.0.0.0',
        'port': 80
    }

    GOOGLE_SENDER_CONF = {
        'FROM_ADDRESS':'',
        'FROM_ADDRESS_PSW':'',
        'SMTP_SERVER':'smtp.gmail.com',
        'SMTP_PORT':'587',
        'TO_LIST':['']
    }

    DB_SETTING = {
        'DB_TYPE':'mysql',
        'DB_NAME':'',
        'CHARSET':'utf8',
    }

    ES_SETTING = {
        'INDEX_INFO':{
            'MYSHIPS':{
                'INDEX_NAME':'',
                'MAPPING_FILEPATH':'/app/lib/es/ship@myships.json'
            },
            'SHIPXY':{
                'INDEX_NAME':'',
                'MAPPING_FILEPATH':'/app/lib/es/ship@shipxy.json'
            }
        },
        'CONNECTION':{}
    }

    CRAWLER_SETTING = {
        'MYSHIPS':{
            'HOST_DOMAIN':'https://www.myships.com/',
            'JS_DEMIX_FILEPATH':'/app/lib/myships_pwd_gen.js'
        },
        'SHIPXY':{
            'HOST_DOMAIN':'http://www.shipxy.com/',
            'JS_DEMIX_FILEPATH':'/app/lib/demix.js',
        }
    }
    CRAWLER_SETTING['MYSHIPS']['AREA_INFO'] = '{}sp/region/latest/shipinfo'.format(CRAWLER_SETTING['MYSHIPS']['HOST_DOMAIN'])
    CRAWLER_SETTING['MYSHIPS']['SHIP_DETAIL'] = '{}sp/ships/posAndAissta'.format(CRAWLER_SETTING['MYSHIPS']['HOST_DOMAIN'])
    CRAWLER_SETTING['MYSHIPS']['LOGIN'] = '{}uc/sys/user/login'.format(CRAWLER_SETTING['MYSHIPS']['HOST_DOMAIN'])

    # http://www.shipxy.com/Home/Login
    CRAWLER_SETTING['SHIPXY']['LOGIN'] = '{}Home/Login'.format(CRAWLER_SETTING['SHIPXY']['HOST_DOMAIN'])
    # http://www.shipxy.com/home/logout
    CRAWLER_SETTING['SHIPXY']['LOGOUT'] = '{}home/logout'.format(CRAWLER_SETTING['SHIPXY']['HOST_DOMAIN'])
    # http://www.shipxy.com/ship/getareashipssimple
    CRAWLER_SETTING['SHIPXY']['DATA_INFO'] = '{}ship/getareashipssimple'.format(CRAWLER_SETTING['SHIPXY']['HOST_DOMAIN'])
    # http://www.shipxy.com/ship/GetShip
    CRAWLER_SETTING['SHIPXY']['SHIP_INFO'] = '{}ship/GetShip'.format(CRAWLER_SETTING['SHIPXY']['HOST_DOMAIN'])

def formal_settings():
    app.config['DB_SETTING']['HOST'] = ''
    app.config['DB_SETTING']['PORT'] = ''
    app.config['DB_SETTING']['ACCOUNT'] = ''
    app.config['DB_SETTING']['PASSWORD'] = ''

    app.config['ES_SETTING']['CONNECTION']['HOST'] = ['']
    app.config['ES_SETTING']['CONNECTION']['PORT'] = 60000
    app.config['ES_SETTING']['CONNECTION']['ACCOUNT'] = ''
    app.config['ES_SETTING']['CONNECTION']['PASSWORD'] = ''

    app.config['LINE_NOTIFY_TOKEN'] = ''

def dev_settings():
    app.config['DB_SETTING']['HOST'] = ''
    app.config['DB_SETTING']['PORT'] = ''
    app.config['DB_SETTING']['ACCOUNT'] = ''
    app.config['DB_SETTING']['PASSWORD'] = ''

    app.config['ES_SETTING']['CONNECTION']['HOST'] = ['']
    app.config['ES_SETTING']['CONNECTION']['PORT'] = 60000
    app.config['ES_SETTING']['CONNECTION']['ACCOUNT'] = ''
    app.config['ES_SETTING']['CONNECTION']['PASSWORD'] = ''

    app.config['LINE_NOTIFY_TOKEN'] = ''

def general_settings():
    app.config['SQLALCHEMY_DATABASE_URI'] = '%(DB_TYPE)s://%(ACCOUNT)s:%(PASSWORD)s@%(HOST)s:%(PORT)s/%(DB_NAME)s?charset=%(CHARSET)s' % app.config['DB_SETTING']
    f = open(app.config['CRAWLER_SETTING']['SHIPXY']['JS_DEMIX_FILEPATH'], 'r')
    app.config['CRAWLER_SETTING']['SHIPXY']['JS_DEMIX_CONTENT'] = f.read()
    f.close()

app.config.from_object('settings.environment.Config')

if not os.environ.get('API_PROPERTY'):
    os.environ['API_PROPERTY'] = 'LOCAL_FORMALITY'

dynamic_settings = {
    'FORMALITY':formal_settings,
    'DEV'      :dev_settings,
}
dynamic_settings[os.environ.get('API_PROPERTY')]()
general_settings()
app.url_map.strict_slashes = False
db = SQLAlchemy(app)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS, GET, PATCH, DELETE, PUT')

    db.session.rollback()
    db.session.close()
    return(response)

@app.teardown_request
def teardown_request(exception):
    if exception:
        db.session.rollback()
    db.session.remove()

try:
    if not database_exists(app.config['SQLALCHEMY_DATABASE_URI']):
        create_database(app.config['SQLALCHEMY_DATABASE_URI'])
except:
    print(traceback.format_exc())