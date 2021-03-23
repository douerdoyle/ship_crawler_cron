# 一台機器在網路正常的狀況下，在 (3241000-1660000)/(29*60-21)=919.720768, 3241000/919.720768/60=58.7315939 分鐘內爬完

if __name__=='__main__':
    import sys
    sys.path.append('../')
import os, traceback, json, time, threading, requests
from requests.auth     import HTTPBasicAuth
from uuid              import uuid4
from datetime          import datetime, timedelta
from pprint            import pprint
from copy              import deepcopy
from lib.es.elastic    import Elastic
from lib.tools         import Myships_Crawler, check_same_process_still_running, get_external_ip, line_notify_pusher, RequestsRetryer
from lib.send_email.gmail_sender import GmailSender
from settings.environment import app, db
from models.area_list  import AreaList
from models.area_ships import SubAreaList
from models.ship_info_mapping import ShipTypeMyships, NavistatusTypeMyships
from models.mmsi_info  import MMSI_Info
from models.account import MyshipsAccount
from schedule.myships_account_login import ship_account_login_func

# posTime：資料時間
# lon：除以600,000後會是經度
# lat：除以600,000後會是緯度
# sog：船的航行速度，單位是「節」，46代表是4.6節
# cog：航向，單位為度，913代表91.3度
# heading：航艏向
# rot：轉向速率
# aisNavStatus：推估可能是航行狀態，1代表 在航(主機推動) ，0代表錨泊，null代表無狀態
# mmsi：MMSI
# shipnameEn：船隻英文名稱
# imo：IMO編號
# callsign：呼號
# shiptype：可能是船隻類型，52代表拖倉，需要抓取更多船隻資料與網頁版做比對才能得知
# length：船隻長度
# breadth：船隻寬度
# eta：timestamp，預計抵達目的地的時間
# destPort：目的地
# draught：吃水，單位為10公分，40代表4.0公尺
# shipId：船隻編號

class myships_crawler_class():
    def __init__(self):
        self.machine_serial = int(os.environ.get('SERIAL', '0'))
        self.script_name = os.path.basename(__file__)
        self.err_msg_list = []
        self.thread_list = []
        self.thread_max_count = 100
        self.err_count = 0
        self.err_count_max = 200
        self.no_data_count = 0
        self.no_data_count_max = 10
        self.machine_count = 1
        self.ship_detail_dict = {}
        self.headers = {
            'Connection':'close',
            'User-Agent':'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
        }
        self.save2es_thread = threading.Thread(target=self.save2es, daemon=True)

    def err_msg_generator(self, err_msg):
        return('\n'.join([datetime.now().strftime('%Y-%m-%d %H:%M:%S'), self.ip,err_msg]))
    
    def save2es(self):
        batch_load_list = []
        for key_id in list(self.ship_detail_dict.keys()):
            id_list = ['{}_{}'.format(ship_detail_dict['mmsi'], ship_detail_dict['posTime']) for ship_detail_dict in self.ship_detail_dict[key_id]]
            if id_list:
                es_ship_ids = set([data['_id'] for data in self.es.scan({'query':{'bool':{'must':[{'terms':{'_id':id_list}}]}}}, app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'])])
            else:
                es_ship_ids = set()
            for ship_detail_dict in self.ship_detail_dict.pop(key_id):
                _id = '{}_{}'.format(ship_detail_dict['mmsi'], ship_detail_dict['posTime'])
                if _id in es_ship_ids:
                    continue
                dictionary = {
                    '_index':app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'],
                    '_type':'_doc',
                    '_id':_id,
                    '_routing':'{}'.format((datetime.utcfromtimestamp(ship_detail_dict['posTime'])+timedelta(hours=8)).year),
                    'updatetime':ship_detail_dict['updatetime'],
                    'eta_timestamp':ship_detail_dict['eta'],
                    'time':(datetime.utcfromtimestamp(ship_detail_dict['posTime'])+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                    'callsign':ship_detail_dict['callsign'],
                    'nationality':self.mmsi_dict[ship_detail_dict['mmsi'][:3]] if ship_detail_dict['mmsi'][:3] in self.mmsi_dict else None,
                    'cog':ship_detail_dict['cog']/10 if ship_detail_dict['cog'] else None,
                    'dest':ship_detail_dict['destPort'],
                    'draught':ship_detail_dict['draught']/10 if ship_detail_dict['draught'] else None,
                    'hdg':ship_detail_dict['heading'],
                    'imo':ship_detail_dict['imo'],
                    'latitude':ship_detail_dict['lat']/600000,
                    'length':ship_detail_dict['length'],
                    'longitude':ship_detail_dict['lon']/600000,
                    'mmsi':ship_detail_dict['mmsi'],
                    'name':ship_detail_dict['shipnameEn'],
                    'navistatus':ship_detail_dict['aisNavStatus'],
                    'rot':ship_detail_dict['rot'],
                    'shipid':ship_detail_dict['shipId'],
                    'sog':ship_detail_dict['sog']/10 if ship_detail_dict['sog'] else None,
                    'utc_timestamp':ship_detail_dict['posTime'],
                    'type':ship_detail_dict['shiptype'],
                    'width':ship_detail_dict['breadth'],
                    'y':ship_detail_dict['shiptype'],
                    'v':ship_detail_dict['aisNavStatus']
                }
                
                try:
                    # type 有時會出現亂碼，如「6857-d&0」、「1607U No.158」
                    dictionary['type'] = int(dictionary['type'])
                except:
                    dictionary['type'] = None
                try:
                    dictionary['navistatus'] = int(dictionary['navistatus'])
                except:
                    dictionary['navistatus'] = None
                if ship_detail_dict['eta']:
                    eta_datetime = datetime.utcfromtimestamp(ship_detail_dict['eta'])
                    dictionary['eta'] = eta_datetime.strftime('%m-%d %H:%M')
                    dictionary['eta_datetime'] = eta_datetime.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    dictionary['eta'] = None
                    dictionary['eta_datetime'] = None
                if dictionary['type'] in self.ship_type_dict:
                    dictionary['type_text'] = self.ship_type_dict[dictionary['type']]
                if dictionary['navistatus'] in self.navistatus_type_dict:
                    dictionary['navistatus_text'] = self.navistatus_type_dict[dictionary['navistatus']]
                dictionary['y'] = dictionary['type']
                dictionary['v'] = dictionary['navistatus']
                batch_load_list.append(dictionary)
        try:
            self.es.batch_load(batch_load_list)
        except:
            self.err_count+=100

    def get_ship_detail(self, shipId_list_for_func):
        input_json = {
            "shipId": ','.join(shipId_list_for_func)
        }
        try:
            rsp = RequestsRetryer('post', {'url':app.config['CRAWLER_SETTING']['MYSHIPS']['SHIP_DETAIL'], 'headers':self.headers, 'json':input_json, 'timeout':180, 'cookies':self.cookies_list}, req_retry_limit=3, req_retry_sleeptime=5)
            rsp.close()
        except:
            print(traceback.format_exc())
            self.err_count+=1
            self.err_msg_list.append(self.err_msg_generator(traceback.format_exc()))
            return
        if rsp.status_code!=200:
            print(rsp.text)
            self.err_count+=1
            self.err_msg_list.append(self.err_msg_generator(rsp.text))
            return
        try:
            rsp_result = rsp.json()
        except:
            print(traceback.format_exc())
            self.err_count+=1
            self.err_msg_list.append(self.err_msg_generator(traceback.format_exc()))
            return
        if rsp_result['code']!='0' \
        or rsp_result['message']!='成功':
            pprint(rsp_result)
            self.err_count+=1
            self.err_msg_list.append(self.err_msg_generator(rsp.text))
            return
        key_id = f'{uuid4()}'
        self.ship_detail_dict[key_id] = []
        for ship_detail_dict in rsp_result['data']:
            if not ship_detail_dict['mmsi'] \
            or not ship_detail_dict['lon'] \
            or not ship_detail_dict['lat'] \
            or not ship_detail_dict['posTime']:
                continue
            ship_detail_dict['v'] = None
            ship_detail_dict['y'] = None
            ship_detail_dict['updatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.ship_detail_dict[key_id].append(ship_detail_dict)
        if not self.ship_detail_dict[key_id]:
            self.no_data_count+=1
            return

    def myships_crawler_func(self):
        print(datetime.now())
        try:
            self.ip = get_external_ip()
        except:
            self.ip = None
        try:
            # 檢查這台機器是否有同排程還在執行
            if check_same_process_still_running(self.script_name):
                # 代表包含這個程式在內，有兩個以上相同的排程正在運行
                print('{}: 有相同排程尚在執行({})'.format(self.script_name, 1))
                return
            if not self.ip:
                raise(Exception('無法取得 IP'))
            try:
                rsp = requests.get(app.config['CRAWLER_SETTING']['MYSHIPS']['HOST_DOMAIN'], timeout=60)
                rsp.close()
            except:
                raise Exception('無法連線至寶船網網頁 :\n{}'.format(traceback.format_exc()))
            if rsp.status_code!=200:
                raise Exception('寶船網網頁無法正確連線 :\n{}'.format(rsp.text))

            try:
                rsp = requests.get('http://{}:{}/'.format('localhost', app.config['ES_SETTING']['CONNECTION']['PORT']), auth=HTTPBasicAuth(app.config['ES_SETTING']['CONNECTION']['ACCOUNT'], app.config['ES_SETTING']['CONNECTION']['PASSWORD']))
                rsp.close()
            except:
                raise Exception(traceback.format_exc())
            if rsp.status_code!=200:
                raise Exception('無法連線至資策會 ES 主機')

            self.es = Elastic(
                    host=app.config['ES_SETTING']['CONNECTION']['HOST'], 
                    port=app.config['ES_SETTING']['CONNECTION']['PORT'], 
                    username=app.config['ES_SETTING']['CONNECTION']['ACCOUNT'],
                    password=app.config['ES_SETTING']['CONNECTION']['PASSWORD']
                    )

            if not self.es.check_index_exist(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME']):
                print(self.es.create_index(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'], app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['MAPPING_FILEPATH']))

            self.ship_type_dict = {x.type:x.name for x in ShipTypeMyships.query.all()}
            self.navistatus_type_dict = {x.type:x.name for x in NavistatusTypeMyships.query.all()}
            self.mmsi_dict = {}
            for db_result in MMSI_Info.query.with_entities(MMSI_Info.mmsi, MMSI_Info.alpha_2, MMSI_Info.alpha_3).all():
                self.mmsi_dict[db_result.mmsi] = db_result.alpha_3 if db_result.alpha_3 else db_result.alpha_2

            print('帳戶檢查登入狀態中')
            account_login_timestamp = time.time()
            account_login_span = 1800
            try:
                ship_account_login_func()
                self.cookies_list = ([x.cookies for x in MyshipsAccount.query.filter(MyshipsAccount.enable==1, MyshipsAccount.updating==0, MyshipsAccount.updated_time>=(datetime.now()-timedelta(hours=1))).all()])
                if not self.cookies_list:
                    self.cookies_list.append({})
            except:
                print('帳號登入失敗')
                print(traceback.format_exc())
                self.cookies_list = [{}]

            # start_n = deepcopy(4000000+self.machine_serial)
            start_n = deepcopy(self.machine_serial)
            # start_n = 1660000
            while True:
                if datetime.now().minute>59 \
                and datetime.now().second>30:
                    return
                print(start_n)

                if (time.time()-account_login_timestamp)>=account_login_span:
                    print(f'帳戶距離上次登入時間超過 {account_login_span} 秒，等待所有 Thread 結束並重新登入後，將繼續執行')
                    for thread in self.thread_list:
                        thread.join()
                    print('帳戶重新登入中')
                    account_login_timestamp = time.time()
                    try:
                        ship_account_login_func()
                        self.cookies_list = ([x.cookies for x in MyshipsAccount.query.filter(MyshipsAccount.enable==1, MyshipsAccount.updating==0, MyshipsAccount.updated_time>=(datetime.now()-timedelta(hours=1))).all()])
                        if not self.cookies_list:
                            self.cookies_list.append({})
                    except:
                        print('帳號登入失敗')
                        print(traceback.format_exc())
                        self.cookies_list = [{}]

                end_n = start_n+1000*self.machine_count
                shipId_list = [f'{i}' for i in range(start_n, end_n, self.machine_count)]
                start_n = deepcopy(end_n)

                t1 = time.time()
                thread = threading.Thread(target=self.get_ship_detail, args=(shipId_list, ), daemon=True)
                thread.start()
                self.thread_list.append(thread)
                thread_sleep_time = 1-(time.time()-t1)
                if thread_sleep_time>0:
                    time.sleep(thread_sleep_time)

                # for thread in self.thread_list:
                #     thread.join()
                # pprint(self.ship_detail_dict)
                # if self.ship_detail_dict:
                #     self.save2es()
                # pprint(self.ship_detail_dict)
                # return

                while [thread.is_alive() for thread in self.thread_list].count(True)>=self.thread_max_count:
                    continue
                delete_index_list = []
                for index, thread in enumerate(self.thread_list):
                    if not thread.is_alive():
                        delete_index_list.append(index)
                delete_index_list.reverse()
                for index in delete_index_list:
                    del(self.thread_list[index])
                if self.err_count>=self.err_count_max:
                    raise Exception('\n\n'.join(self.err_msg_list))
                if self.no_data_count>=self.no_data_count_max:
                    break
                if self.ship_detail_dict and not self.save2es_thread.is_alive():
                    self.save2es_thread = threading.Thread(target=self.save2es, daemon=True)
                    self.save2es_thread.start()
            print('完成爬取，等待 Thread 結束')
            for thread in self.thread_list:
                thread.join()
            print('Thread 結束, 正在將最後剩餘資料存入 ES 中')
            while self.save2es_thread.is_alive():
                continue
            if self.ship_detail_dict:
                self.save2es()
            print('結束')
            print(datetime.now())
            exit()
        except:
            msg = '\n\n'.join(['ip: {}'.format(self.ip), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()])
            print(msg)
            self.err_msg_list.append(msg)
            self.err_msg_list = list(set(self.err_msg_list))
            print('\n\n'.join(self.err_msg_list))
            # ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(self.err_msg_list))
            # ggg.send_email()
            # line_notify_pusher(msg)

if __name__ == "__main__":
    # if 1<=datetime.now().minute<=20 \
    # or 31<=datetime.now().minute<=50:
    #     try:
    #         mcc = myships_crawler_class()
    #         mcc_mcf = threading.Thread(target=mcc.myships_crawler_func, daemon=True)
    #         mcc_mcf.start()
    #     except:
    #         print(traceback.format_exc())
    # while True:
    #     if datetime.now().minute in [0, 30]:
    #         try:
    #             mcc = myships_crawler_class()
    #             mcc_mcf = threading.Thread(target=mcc.myships_crawler_func, daemon=True)
    #             mcc_mcf.start()
    #         except:
    #             print(traceback.format_exc())
    #             continue
    #         time.sleep(60)

    while True:
        start_timestamp = time.time()
        try:
            mcc = myships_crawler_class()
            mcc.myships_crawler_func()
        except:
            print(traceback.format_exc())
            continue
        sleep_time = time.time()-start_timestamp
        if sleep_time<1800:
            time.sleep(1800-sleep_time)