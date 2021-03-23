if __name__=='__main__':
    import sys
    sys.path.append('../')
import os, traceback, json, time, sqlalchemy
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
    'callsign': 'callsign',
    # 'e': 'cnname',
    'f': 'nationality',
    'cog': 'cog',
    'destPort': 'dest',
    'draught': 'draught',
    'heading': 'hdg',
    'imo': 'imo',
    'lat': 'latitude',
    'length': 'length',
    'lon': 'longitude',
    'mmsi': 'mmsi',
    'shipnameEn': 'name',
    'aisNavStatus': 'navistatus',
    'rot': 'rot',
    'shipId': 'shipid',
    'sog': 'sog',
    'posTime': 'utc_timestamp',
    'shiptype': 'type',
    'breadth': 'width',
}

def myships_crawler_func():
    script_name = os.path.basename(__file__)
    # 檢查這台機器是否有同排程還在執行
    if check_same_process_still_running(script_name):
        # 代表包含這個程式在內，有兩個以上相同的排程正在運行
        print('{}: 有相同排程尚在執行({})'.format(script_name, 1))
        return
    try:
        es = Elastic(
                host=app.config['ES_SETTING']['CONNECTION']['HOST'], 
                port=app.config['ES_SETTING']['CONNECTION']['PORT'], 
                username=app.config['ES_SETTING']['CONNECTION']['ACCOUNT'],
                password=app.config['ES_SETTING']['CONNECTION']['PASSWORD']
                )

        if not es.check_index_exist(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME']):
            print(es.create_index(app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'], app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['MAPPING_FILEPATH']))

        db_result_list = AreaList.query.with_entities(AreaList.id, AreaList.crawl_span).order_by(asc(AreaList.crawl_span)).all()
        if not db_result_list:
            print('{}: 無區域的排程區間資料'.format(script_name))
            return
        crawl_span_dict = {db_result.id:db_result.crawl_span for db_result in db_result_list}
        query_sort_conds = [SubAreaList.area_list_id]
        query_sort_conds.extend([x.id for x in db_result_list])

        cold_zone_ids = set([db_result.id for db_result in AreaList.query.filter(AreaList.enable==1, AreaList.name.like('%冷區%')).all()])

        ship_type_dict = {x.type:x.name for x in ShipTypeMyships.query.all()}
        navistatus_type_dict = {x.type:x.name for x in NavistatusTypeMyships.query.all()}

        mc = Myships_Crawler()
        while True:
            db.session.rollback()
            db.session.close()
            batch_load_list = []

            db_result = SubAreaList.query.filter(
                    SubAreaList.enable==1, 
                    SubAreaList.web=='myships',
                    or_(SubAreaList.next_time<=datetime.now(), 
                    SubAreaList.next_time==None), 
                    or_(*[SubAreaList.area_list_id==id for id in crawl_span_dict.keys()])
                ).order_by(
                    sqlalchemy.func.field(*query_sort_conds), 
                    asc(SubAreaList.next_time),
                    func.random()
                ).first()
            if not db_result:
                print('{}: 完成'.format(script_name))
                return
            print('{}: 爬取區域 {} 中'.format(script_name, db_result.id))
            crawler_time = datetime.now()-timedelta(minutes=datetime.now().minute%crawl_span_dict[db_result.area_list_id])
            old_crawler_time = deepcopy(db_result.crawler_time)
            old_next_time = deepcopy(db_result.next_time)
            db_result.crawler_time = datetime.strptime(crawler_time.strftime('%Y-%m-%d %H:%M:00'), '%Y-%m-%d %H:%M:%S')
            db_result.next_time = db_result.crawler_time+timedelta(minutes=crawl_span_dict[db_result.area_list_id])
            db.session.add(db_result)
            db.session.commit()

            if db_result.lu_lat==db_result.rd_lat \
            or db_result.lu_lng==db_result.rd_lng:
                continue

            # ma_cookies_list = [x.cookies for x in MyshipsAccount.query.filter(MyshipsAccount.enable==1, MyshipsAccount.updating==0).all()]
            ma_cookies_list = []
            try:
                area_result = mc.area_info(min([db_result.lu_lat, db_result.rd_lat]), min([db_result.lu_lng, db_result.rd_lng]), max([db_result.lu_lat, db_result.rd_lat]), max([db_result.lu_lng, db_result.rd_lng]), ma_cookies_list)
            except:
                db_result.crawler_time = old_crawler_time
                db_result.next_time = old_next_time
                db.session.add(db_result)
                db.session.commit()

                msg = '\n\n'.join(
                    [
                        'ip: {}'.format(get_external_ip()), 
                        '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                        '{}\n{}'.format('取得區域船隻資料出現錯誤，請檢查是資策會端網路出現錯誤還是寶船網網站異常', traceback.format_exc()),
                        
                    ]
                )
                print(msg)
                ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
                ggg.send_email()
                continue

            if area_result['code']!='0':
                msg = '\n\n'.join(
                    [
                        'ip: {}'.format(get_external_ip()), 
                        '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                        '\n'.join(
                            [
                                '取得寶船網區域船隻資料時出現錯誤',
                                '{}'.format(area_result),
                                'id :{}'.format(db_result.id),
                                'area_list_id :{}'.format(db_result.area_list_id),
                                '{}'.format({'age':1440, 'rgn':mc.check_trans2myships_coord([[min([db_result.lu_lat, db_result.rd_lat]), min([db_result.lu_lng, db_result.rd_lng])], [max([db_result.lu_lat, db_result.rd_lat]), max([db_result.lu_lng, db_result.rd_lng])]])}),
                                '{}'.format([[db_result.lu_lat, db_result.lu_lng], [db_result.rd_lat, db_result.rd_lng]])
                            ]
                        )
                    ]
                )
                print(msg)
                ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
                ggg.send_email()
                continue

            tmp_area_data_list = area_result.pop('data')
            area_result['data'] = []
            
            for area_data in tmp_area_data_list:
                if not area_data.get('m') \
                or area_data['m']=='0':
                    continue
                area_result['data'].append(area_data)

            # 該區域沒有任何船隻資料的話，略過
            if not area_result['data']:
                print('{}: Skip Area {}'.format(script_name, db_result.id))
                continue
            else:
                print('{}: 區域 {} 有 {} 艘船隻'.format(script_name, db_result.id, len(area_result['data'])))

            id_list = []
            ship_data_dict = mc.ship_info([area_data['i'] for area_data in area_result['data']])
            for area_data in area_result['data']:
                if area_data['i'] not in ship_data_dict:
                    print(area_data['i'])
                    continue
                id_list.append('{}_{}'.format(area_data['m'], ship_data_dict[area_data['i']]['posTime']))
            if id_list:
                es_ship_ids = set([data['_id'] for data in es.scan({'query':{'bool':{'must':[{'terms':{'_id':id_list}}]}}}, app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'])])
            else:
                es_ship_ids = set()

            for area_data in area_result['data']:
                if area_data['i'] not in ship_data_dict:
                    print(area_data['i'])
                    continue
                ship_data = ship_data_dict[area_data['i']]
                try:
                    dictionary = {
                        '_index':app.config['ES_SETTING']['INDEX_INFO']['MYSHIPS']['INDEX_NAME'],
                        '_type':'_doc',
                        '_id':'{}_{}'.format(area_data['m'], ship_data['posTime']),
                        '_routing':'{}'.format((datetime.utcfromtimestamp(ship_data['posTime'])+timedelta(hours=8)).year) if ship_data['posTime'] else None,
                        'updatetime':ship_data['updatetime'] if ship_data.get('updatetime') else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'eta_timestamp':ship_data['eta'],
                        'eta':area_data['r'],
                        'time':(datetime.utcfromtimestamp(ship_data['posTime'])+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                        'y':area_data['y']
                    }
                except:
                    msg = '\n'.join([traceback.format_exc(), '{}'.format(area_data), '{}'.format(ship_data)])
                    ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
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

                if dictionary['type'] not in ship_type_dict:
                    sts_db_result = ShipTypeMyships.query.filter(ShipTypeMyships.type==dictionary['type']).first()
                    if sts_db_result:
                        ship_type_dict[sts_db_result.type] = sts_db_result.name
                    else:
                        ship_type_dict[dictionary['type']] = None
                dictionary['type_text'] = ship_type_dict[dictionary['type']]

                if dictionary['navistatus'] not in navistatus_type_dict:
                    nt_db_result = NavistatusTypeMyships.query.filter(NavistatusTypeMyships.type==dictionary['navistatus']).first()
                    if nt_db_result:
                        navistatus_type_dict[nt_db_result.type] = nt_db_result.name
                    else:
                        navistatus_type_dict[dictionary['navistatus']] = None
                dictionary['navistatus_text'] = navistatus_type_dict[dictionary['navistatus']]

                batch_load_list.append(dictionary)

            if batch_load_list:
                es.batch_load(batch_load_list)
            # #############################
            # if len(batch_load_list)>2:
            #     return
            # #############################
    except:
        msg = '\n\n'.join(['ip: {}'.format(get_external_ip()), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()])
        print(msg)
        ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
        ggg.send_email()
        # line_notify_pusher(msg)

if __name__ == "__main__":
    myships_crawler_func()