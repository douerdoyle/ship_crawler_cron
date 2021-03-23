if __name__=='__main__':
    import sys
    sys.path.append('../')
import os, traceback, json, time, sqlalchemy, threading
from datetime          import datetime, timedelta
from pprint            import pprint
from copy              import deepcopy
from sqlalchemy        import asc, or_, func
from lib.es.elastic    import Elastic
from lib.tools         import Myships_Crawler, check_same_process_still_running, get_external_ip, line_notify_pusher
from lib.send_email.gmail_sender import GmailSender
from settings.environment import app, db
from models.area_list  import AreaList
from models.area_ships import SubAreaList
from models.ship_info_mapping import ShipTypeMyships, NavistatusTypeMyships
from models.account import MyshipsAccount

# i：船隻編號，可以用其取得船隻詳細資訊
# t：timestamp，資料時間
# e：船隻名稱
# m：MMSI
# n：除以600,000後會是經度
# a：除以600,000後會是緯度
# f：船隻國籍
# y：未知
# l：船長度，單位為公尺
# c：航向，單位為度，如2213代表221.3度
# s：船速，單位是節，75代表7.5節
# h：航艏向，單位為度，221代表為221度
# v：推估可能是航行狀態，1代表 在航(主機推動) ，0代表錨泊，null代表無狀態
# g：呼號
# o：IMO
# b：船寬，單位為公尺
# r：預計抵達目的地的時間，格式會是 05-09 12:00 ，時間會是UTC時間，要再加8小時才會是台時間
# p：目的地，無固定格式，有些前往香港會是寫 HKG，有些會寫 HKG===KHS
# d：吃水，單位為公尺

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

format_data_content = {
    'draught':10,
    'latitude':600000,
    'longitude':600000,
    'cog':10,
    'sog':10,
}

format_batch_load_dict = {
    'g': 'callsign',
    'f': 'nationality',
    'c': 'cog',
    'p': 'dest',
    'd': 'draught',
    'h': 'hdg',
    'o': 'imo',
    'a': 'latitude',
    'l': 'length',
    'n': 'longitude',
    'm': 'mmsi',
    'e': 'name',
    'v': 'navistatus',
    'rot': 'rot',
    'i': 'shipid',
    's': 'sog',
    't': 'utc_timestamp',
    'y': 'type',
    'b': 'width',
}

# format_batch_load_dict = {
#     'callsign': 'callsign',
#     'f': 'nationality',
#     'cog': 'cog',
#     'destPort': 'dest',
#     'draught': 'draught',
#     'heading': 'hdg',
#     'imo': 'imo',
#     'lat': 'latitude',
#     'length': 'length',
#     'lon': 'longitude',
#     'mmsi': 'mmsi',
#     'shipnameEn': 'name',
#     'aisNavStatus': 'navistatus',
#     'rot': 'rot',
#     'shipId': 'shipid',
#     'sog': 'sog',
#     'posTime': 'utc_timestamp',
#     'shiptype': 'type',
#     'breadth': 'width',
# }

class myships_crawler_class():
    def __init__(self):
        self.script_name = os.path.basename(__file__)
        self.thread_error_count = 0
        self.err_msg_list = []
        self.thread_max_count = 20
        self.thread_list = []
        self.rollback_id_list = []
        self.thread_error_count_max = self.thread_max_count*2
        self.time_dict = {
            'old_crawler_time':{},
            'old_next_time':{}
        }

    def myships_thread(self, db_result_dict):
        try:
            batch_load_list = []
            try:
                area_result = self.mc.area_info(min([db_result_dict['lu_lat'], db_result_dict['rd_lat']]), min([db_result_dict['lu_lng'], db_result_dict['rd_lng']]), max([db_result_dict['lu_lat'], db_result_dict['rd_lat']]), max([db_result_dict['lu_lng'], db_result_dict['rd_lng']]))
            except:
                self.thread_error_count+=1
                self.rollback_id_list.append(db_result_dict['id'])
                msg = '\n\n'.join(
                    [
                        'ip: {}'.format(get_external_ip()), 
                        '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                        '{}\n{}'.format('取得區域船隻資料出現錯誤，請檢查是資策會端網路出現錯誤還是寶船網網站異常', traceback.format_exc()),
                        
                    ]
                )
                print(msg)
                self.err_msg_list.append(msg)
                # ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
                # ggg.send_email()
                return

            if area_result['code']!='0':
                self.thread_error_count+=1
                msg = '\n\n'.join(
                    [
                        'ip: {}'.format(get_external_ip()), 
                        '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                        '\n'.join(
                            [
                                '取得寶船網區域船隻資料時出現錯誤',
                                '{}'.format(area_result),
                                'id :{}'.format(db_result_dict['id']),
                                'area_list_id :{}'.format(db_result_dict['area_list_id']),
                                '{}'.format([[db_result_dict['lu_lat'], db_result_dict['lu_lng']], [db_result_dict['rd_lat'], db_result_dict['rd_lng']]])
                            ]
                        )
                    ]
                )
                print(msg)
                self.err_msg_list.append(msg)
                # ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
                # ggg.send_email()
                return

            tmp_area_data_list = area_result.pop('data')
            area_result['data'] = []
            
            for area_data in tmp_area_data_list:
                if not area_data.get('m') \
                or area_data['m']=='0':
                    continue
                area_result['data'].append(area_data)

            # 該區域沒有任何船隻資料的話，略過
            if not area_result['data']:
                print('{}: Skip Area {}'.format(self.script_name, db_result_dict['id']))
                return
            else:
                print('{}: 區域 {} 有 {} 艘船隻'.format(self.script_name, db_result_dict['id'], len(area_result['data'])))

            id_list = []
            ship_data_dict = self.mc.ship_info([area_data['i'] for area_data in area_result['data']])
            for area_data in area_result['data']:
                if area_data['i'] not in ship_data_dict:
                    print(area_data['i'])
                    continue
                id_list.append('{}_{}'.format(area_data['m'], area_data['t']))
            if id_list:
                es_ship_ids = set([data['_id'] for data in self.es.scan({'query':{'bool':{'must':[{'terms':{'_id':id_list}}]}}}, app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'])])
            else:
                es_ship_ids = set()

            for area_data in area_result['data']:
                if area_data['i'] not in ship_data_dict:
                    print(area_data['i'])
                    continue
                # 有時候拉船隻詳細資料 posTime 會是 Null, 這時改為區域船隻資料的船隻資料時間點
                ship_data = ship_data_dict[area_data['i']]
                try:
                    dictionary = {
                        '_index':app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'],
                        '_type':'_doc',
                        '_id':'{}_{}'.format(area_data['m'], area_data['t']),
                        '_routing':'{}'.format((datetime.utcfromtimestamp(area_data['t'])+timedelta(hours=8)).year) if area_data['t'] else None,
                        'updatetime':area_data['updatetime'] if area_data.get('updatetime') else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'eta_timestamp':ship_data['eta'],
                        'eta':area_data['r'],
                        'time':(datetime.utcfromtimestamp(area_data['t'])+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                        'y':area_data['y']
                    }
                except:
                    self.thread_error_count+=1
                    msg = '\n'.join([traceback.format_exc(), '{}'.format(area_data), '{}'.format(ship_data)])
                    print(msg)
                    self.err_msg_list.append(msg)
                    ggg = GmailSender('船隻爬蟲船隻資料出現異常-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
                    ggg.send_email()
                    continue

                if dictionary['_id'] in es_ship_ids:
                    continue

                try:
                    dictionary['v'] = int(area_data['v'])
                except:
                    dictionary['v'] = None

                if not dictionary['eta_timestamp']:
                    dictionary['eta_timestamp'] = None
                    dictionary['eta_datetime'] = None
                else:
                    dictionary['eta_datetime'] = (datetime.utcfromtimestamp(dictionary['eta_timestamp'])+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

                for source_key, new_key in format_batch_load_dict.items():
                    if source_key in area_data:
                        dictionary[new_key] = area_data[source_key]
                    elif source_key in ship_data:
                        dictionary[new_key] = ship_data[source_key]

                dictionary['shipid'] = '{}'.format(dictionary['shipid'])

                for key, divisor in format_data_content.items():
                    if dictionary.get(key):
                        dictionary[key] = round(dictionary[key]/divisor, 6)

                for key in list(dictionary.keys()):
                    if type(dictionary[key]) is not str:
                        continue
                    dictionary[key] = dictionary[key].strip()
                    if not dictionary[key] or dictionary[key]=='NULL':
                        dictionary[key] = None
                for key in ['navistatus', 'rot', 'type', 'y']:
                    if dictionary.get(key) and type(dictionary[key] is not int):
                        dictionary[key] = int(dictionary[key])

                if dictionary['type'] not in self.ship_type_dict:
                    sts_db_result = ShipTypeMyships.query.filter(ShipTypeMyships.type==dictionary['type']).first()
                    if sts_db_result:
                        self.ship_type_dict[sts_db_result.type] = sts_db_result.name
                    else:
                        self.ship_type_dict[dictionary['type']] = None
                dictionary['type_text'] = self.ship_type_dict[dictionary['type']]

                if dictionary['navistatus'] not in self.navistatus_type_dict:
                    nt_db_result = NavistatusTypeMyships.query.filter(NavistatusTypeMyships.type==dictionary['navistatus']).first()
                    if nt_db_result:
                        self.navistatus_type_dict[nt_db_result.type] = nt_db_result.name
                    else:
                        self.navistatus_type_dict[dictionary['navistatus']] = None
                dictionary['navistatus_text'] = self.navistatus_type_dict[dictionary['navistatus']]

                batch_load_list.append(dictionary)
            if batch_load_list:
                self.es.batch_load(batch_load_list)
        except:
            self.thread_error_count+=1
            msg = traceback.format_exc()
            print(msg)
            self.err_msg_list.append(msg)

    def myships_crawler_func(self):
        # 檢查這台機器是否有同排程還在執行
        if check_same_process_still_running(self.script_name):
            # 代表包含這個程式在內，有兩個以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(self.script_name, 1))
            return
        try:
            self.es = Elastic(
                    host=app.config['ES_SETTING']['CONNECTION']['HOST'], 
                    port=app.config['ES_SETTING']['CONNECTION']['PORT'], 
                    username=app.config['ES_SETTING']['CONNECTION']['ACCOUNT'],
                    password=app.config['ES_SETTING']['CONNECTION']['PASSWORD']
                    )

            if not self.es.check_index_exist(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME']):
                print(self.es.create_index(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'], app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['MAPPING_FILEPATH']))

            db_result_list = AreaList.query.with_entities(AreaList.id, AreaList.crawl_span).order_by(asc(AreaList.crawl_span)).all()
            if not db_result_list:
                print('{}: 無區域的排程區間資料'.format(self.script_name))
                return
            self.crawl_span_dict = {db_result.id:db_result.crawl_span for db_result in db_result_list}
            self.query_sort_conds = [SubAreaList.area_list_id]
            self.query_sort_conds.extend([x.id for x in db_result_list])

            self.ship_type_dict = {x.type:x.name for x in ShipTypeMyships.query.all()}
            self.navistatus_type_dict = {x.type:x.name for x in NavistatusTypeMyships.query.all()}

            self.mc = Myships_Crawler()
            while True:
                if datetime.now().minute>57 \
                and datetime.now().second>30:
                    return
                db.session.rollback()
                db.session.close()

                if self.thread_error_count>=self.thread_error_count_max:
                    self.err_msg_list = list(set(self.err_msg_list))
                    raise Exception('\n\n'.join(self.err_msg_list))

                del_index_list = []
                for index, thread in enumerate(self.thread_list):
                    if not thread.is_alive():
                        del_index_list.append(index)
                del_index_list.reverse()
                for index in del_index_list:
                    del(self.thread_list[index])
                if self.rollback_id_list:
                    while self.rollback_id_list:
                        db_result = SubAreaList.query.filter(SubAreaList.id==self.rollback_id_list[0]).first()
                        db_result.crawler_time = self.time_dict['old_crawler_time'][self.rollback_id_list[0]]
                        db_result.next_time = self.time_dict['old_next_time'][self.rollback_id_list[0]]
                        db.session.add(db_result)
                        del(self.time_dict['old_crawler_time'][self.rollback_id_list[0]])
                        del(self.time_dict['old_next_time'][self.rollback_id_list[0]])
                        del(self.rollback_id_list[0])
                    db.session.commit()

                db_result = SubAreaList.query.filter(
                        SubAreaList.enable==1, 
                        SubAreaList.web=='myships',
                        or_(SubAreaList.next_time<=datetime.now(), 
                        SubAreaList.next_time==None), 
                        or_(*[SubAreaList.area_list_id==id for id in self.crawl_span_dict.keys()])
                    ).order_by(
                        sqlalchemy.func.field(*self.query_sort_conds), 
                        asc(SubAreaList.next_time),
                        func.random()
                    ).first()
                if not db_result:
                    if self.thread_list:
                        for thread in self.thread_list:
                            thread.join()
                        continue
                    print('{}: 完成'.format(self.script_name))
                    return
                print('{}: 爬取區域 {} 中'.format(self.script_name, db_result.id))
                crawler_time = datetime.now()-timedelta(minutes=datetime.now().minute%self.crawl_span_dict[db_result.area_list_id])
                self.time_dict['old_crawler_time'][db_result.id] = deepcopy(db_result.crawler_time)
                self.time_dict['old_next_time'][db_result.id] = deepcopy(db_result.next_time)
                db_result.crawler_time = datetime.strptime(crawler_time.strftime('%Y-%m-%d %H:%M:00'), '%Y-%m-%d %H:%M:%S')
                db_result.next_time = db_result.crawler_time+timedelta(minutes=self.crawl_span_dict[db_result.area_list_id])
                db.session.add(db_result)
                db.session.commit()

                if db_result.lu_lat==db_result.rd_lat \
                or db_result.lu_lng==db_result.rd_lng:
                    continue

                self.mc.set_cookies_list([x.cookies for x in MyshipsAccount.query.filter(MyshipsAccount.enable==1, MyshipsAccount.updating==0, MyshipsAccount.updated_time>=(datetime.now()-timedelta(hours=1))).all()])

                thread = threading.Thread(target=self.myships_thread, args=(db_result.json(), ), daemon=True)
                thread.start()
                time.sleep(2)
                self.thread_list.append(thread)
                while [x.is_alive() for x in self.thread_list].count(True)>=self.thread_max_count:
                    continue
                
            if self.err_msg_list:
                self.err_msg_list = list(set(self.err_msg_list))
                ggg = GmailSender('船隻爬蟲 {} 執行完成，但途中有部份錯誤'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(self.err_msg_list))
                ggg.send_email()
        except:
            msg = '\n\n'.join(['ip: {}'.format(get_external_ip()), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()])
            print(msg)
            self.err_msg_list.append(msg)
            self.err_msg_list = list(set(self.err_msg_list))
            ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(self.err_msg_list))
            ggg.send_email()
            # line_notify_pusher(msg)

if __name__ == "__main__":
    mcc = myships_crawler_class()
    mcc.myships_crawler_func()