import sys
if __name__=='__main__':
    sys.path.append('../')
import traceback, requests, json, time, os, math, threading, sqlalchemy
from datetime          import datetime, timedelta
from copy              import deepcopy
from uuid              import uuid1
from shutil            import copyfile, move
from pprint            import pprint
from sqlalchemy        import asc, or_, func
from lib.es.elastic    import Elastic
from lib.tools         import ShipXY_Crawler, check_same_process_still_running, get_external_ip, line_notify_pusher, s2tw_converter
from lib.send_email.gmail_sender import GmailSender
from settings.environment import app, db
from models.area_list  import AreaList
# from models.area_ships import AreaShipxy
from models.area_ships import SubAreaList
from models.mmsi_info  import MMSI_Info
from models.ship_info_mapping import ShipTypeShipxy, NavistatusTypeShipxy
from models.crawler_machine import CrawlerMachine
from models.account import ShipxyAccount

class get_ship_detail_class():
    def __init__(self, sc_for_gsdc):
        self.script_name = os.path.basename(__file__)
        self.sc = sc_for_gsdc
        self.thread_result_dict = {}
        self.error_msg_list = []
        self.get_ship_detail_thread_list = []

    def get_ship_detail(self, area_data_for_func):
        try:
            ship_info_result = self.sc.ship_info('{}'.format(area_data_for_func['mmsi']))
            # print(ship_info_result)
            self.thread_result_dict.update({area_data_for_func['mmsi']:ship_info_result['data'][0]})
            self.thread_result_dict[area_data_for_func['mmsi']]['updatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        except:
            self.error_msg_list.append(traceback.format_exc())
            # print(traceback.format_exc())

def time_to_stop():
    return(True if datetime.now().minute>=59 and datetime.now().second>=30 else False)

class shipxy_crawler_class():
    def __init__(self):
        self.crawler_status = True
        self.error_msg_list = []
        self.db_rollback_dict = {}
        self.script_name = os.path.basename(__file__)
        self.get_shipxy_thread_list = []
        # 查詢船隻詳細資料至少要有 6 個thread，才有辦法在一小時內把冷熱區爬完
        self.get_ship_detail_quantity_limit = 6

        self.ship_type_dict = {x.type:x.name for x in ShipTypeShipxy.query.all()}
        self.navistatus_type_dict = {x.type:x.name for x in NavistatusTypeShipxy.query.all()}

    def get_shipxy_thread(self, db_result_dict_for_func):
        try:
            # data_token_result = self.sc.getareashipssimple(db_result_dict_for_func['coor_1'], db_result_dict_for_func['coor_2'])
            data_token_result = self.sc.getareashipssimple([db_result_dict_for_func['lu_lat'], db_result_dict_for_func['lu_lng']], [db_result_dict_for_func['rd_lat'], db_result_dict_for_func['rd_lng']])
        except:
            print(traceback.format_exc())
            self.db_rollback_dict[db_result_dict_for_func['id']] = db_result_dict_for_func
        if db_result_dict_for_func['id'] in self.db_rollback_dict:
            return
        try:
            time.sleep(self.gernal_sleep_time)
            if db_result_dict_for_func['id'] in self.db_rollback_dict:
                return
            batch_load_list = []

            if data_token_result['status']!=0 \
            or 'data' not in data_token_result:
                self.crawler_status = False
                self.error_msg_list.append('{}: 取得data token失敗, 船訊網 API 回傳 {}'.format(self.script_name, data_token_result))
                return

            if not data_token_result['count']:
                print('{}: 區域 {} 內無船隻資料'.format(self.script_name, db_result_dict_for_func['id']))
                return

            area_result_list = []
            area_data_list = self.sc.area_info(data_token_result['data'])
            for area_data in area_data_list:
                if not area_data.get('mmsi') \
                or area_data['mmsi']==0 \
                or area_data['mmsi']=='0':
                    continue
                area_result_list.append(area_data)

            # if len(area_data_list)!=len(area_result_list):
            #     print(
            #         '\n'.join(
            #             [
            #                 '-'*20,
            #                 '{}/{}'.format(len(area_data_list), len(area_result_list)),
            #                 '{}, {}'.format(db_result_dict_for_func['lu_lat'], db_result_dict_for_func['lu_lng']),
            #                 '{}, {}'.format(db_result_dict_for_func['rd_lat'], db_result_dict_for_func['rd_lng']),
            #                 json.dumps(area_data_list, ensure_ascii=False, indent=4),
            #                 '-'*20
            #             ]
            #         )
            #     )

            if not area_result_list:
                print('{}: 區域 {} 內無可爬的船隻資料'.format(self.script_name, db_result_dict_for_func['id']))
                self.es.batch_load(batch_load_list)
                return

            gsdc = get_ship_detail_class(self.sc)
            thread_start_time = time.time()

            for index, area_data in enumerate(area_result_list):
                # 爬太久的停止機制
                if time_to_stop():
                    break
                try:
                    # 每個帳號，至少隔 1 秒requests一次，避免帳號被鎖
                    while [x.is_alive() for x in gsdc.get_ship_detail_thread_list].count(True)>=math.floor((self.get_shipxy_thread_limit_tmp*self.get_ship_detail_quantity_limit)/([x.is_alive() for x in self.get_shipxy_thread_list].count(True)+1)):
                        # 爬太久的停止機制
                        if time_to_stop():
                            break
                        continue
                except:
                    lll = [
                        traceback.format_exc(),
                        '{}'.format([x.is_alive() for x in gsdc.get_ship_detail_thread_list].count(True)),
                        '{}'.format(self.get_shipxy_thread_limit_tmp),
                        '{}'.format(self.get_ship_detail_quantity_limit),
                        '{}'.format([x.is_alive() for x in self.get_shipxy_thread_list].count(True)),
                    ]
                    raise Exception('\n'.join(lll))
                remove_index_list = []
                for index, thread in enumerate(gsdc.get_ship_detail_thread_list):
                    if not thread.is_alive():
                        remove_index_list.append(index)
                remove_index_list.reverse()
                for index in remove_index_list:
                    del(gsdc.get_ship_detail_thread_list[index])
                thread = threading.Thread(target=gsdc.get_ship_detail,args=(area_data, ), daemon=True)
                thread.start()
                gsdc.get_ship_detail_thread_list.append(thread)
                time.sleep(self.gernal_sleep_time)

            while [x.is_alive() for x in gsdc.get_ship_detail_thread_list].count(True):
                # 爬太久的停止機制
                if time_to_stop():
                    break
                continue

            print('爬取區域: {}, 耗費時間: {}, 船隻數量: {}'.format(db_result_dict_for_func['id'], round((time.time()-thread_start_time), 1), len(area_result_list)))

            # 爬太久的停止機制
            if not time_to_stop():
                if len(area_result_list)>=100 and not gsdc.thread_result_dict:
                    self.crawler_status = False
                    self.error_msg_list.append('\n'.join(list(set(gsdc.error_msg_list))))
                    return
                elif len(area_result_list)>=100 and int(len(area_result_list)*0.8)>len(list(gsdc.thread_result_dict.keys())):
                    self.db_rollback_dict[db_result_dict_for_func['id']] = db_result_dict_for_func
                    self.error_msg_list.append('\n'.join(list(set(gsdc.error_msg_list))))
                    return

            id_list = []
            for area_data in area_result_list:
                if area_data['mmsi'] not in gsdc.thread_result_dict:
                    continue
                id_list.append('{}_{}'.format(area_data['mmsi'], gsdc.thread_result_dict[area_data['mmsi']]['lastdyn']))
            if id_list:
                es_ship_ids = set([data['_id'] for data in self.es.scan({'query':{'bool':{'must':[{'terms':{'_id':id_list}}]}}}, app.config['ES_SETTING']['INDEX_INFO']['SHIPXY']['INDEX_NAME'])])
            else:
                es_ship_ids = set()

            # print(
            #     '\n'.join(
            #         [
            #             'area_result_list len: {}'.format(len(area_result_list)),
            #             'id_list len: {}'.format(len(id_list)),
            #             'es_ship_ids len: {}'.format(len(list(es_ship_ids))),
            #             'area_result_list-id_list= {}'.format(len(area_result_list)-len(id_list)),
            #             'id_list-es_ship_ids= {}'.format(len(id_list)-len(list(es_ship_ids)))
            #         ]
            #     )
            # )

            delete_list = []
            for index, area_data in enumerate(area_result_list):
                if area_data['mmsi'] not in gsdc.thread_result_dict:
                    print('{} : 未取得船隻 {} 之詳細資訊，略過之'.format(self.script_name, area_data['mmsi']))
                    continue
                dictionary = deepcopy(gsdc.thread_result_dict[area_data['mmsi']])
                dictionary['latitude'] = area_data['lat'] # 緯度
                dictionary['longitude'] = area_data['lng'] # 經度

                dictionary = deepcopy(gsdc.thread_result_dict[area_data['mmsi']])
                dictionary['_index'] = app.config['ES_SETTING']['INDEX_INFO']['SHIPXY']['INDEX_NAME']
                dictionary['_type'] = '_doc'
                dictionary['_id'] = '{}_{}'.format(area_data['mmsi'], dictionary['lastdyn'])

                if dictionary['_id'] in es_ship_ids:
                    continue

                # dictionary['area_list_id'] = db_result_dict_for_func['area_list_id']

                dictionary['nationality'] = self.mmsi_dict[dictionary['mmsi'][:3]] if dictionary['mmsi'][:3] in self.mmsi_dict else None

                dictionary['cog'] = dictionary['cog']/100 # 對地航向
                dictionary['draught'] = dictionary['draught']/1000 # 吃水
                dictionary['hdg'] = dictionary['hdg']/100 # 船首向
                # # 船訊網可能會出現heading被設為51100，網頁上船首向為0的狀況
                # if dictionary['hdg']>360:
                #     dictionary['hdg'] = 0
                for key in ['lat', 'lon']:
                    dictionary.pop(key)
                dictionary['latitude'] = area_data['lat'] # 緯度
                dictionary['longitude'] = area_data['lng'] # 經度
                dictionary['sog'] = round(dictionary['sog']/5133*10, 2) # 速度：節
                dictionary['length'] = dictionary['length']/10 # 船長
                dictionary['lineWidth'] = area_data['lineWidth']
                dictionary['width'] = dictionary['width']/10 # 船寬

                dictionary['lastdyn_active'] = area_data['lastdyn_active'] # 是否可擷取資料
                dictionary['offset'] = area_data['offset']
                dictionary['rot'] = area_data.get('rot')
                dictionary['rotate'] = area_data['rotate']
                dictionary['shiptype'] = area_data['shiptype']
                dictionary['state'] = area_data['state']
                dictionary['state_color'] = area_data['state_color']
                dictionary['istop'] = area_data['istop']
                dictionary['tracks'] = area_data['tracks']

                dictionary['tcname'] = s2tw_converter(dictionary['cnname'])

                dictionary['utc_timestamp'] = dictionary.pop('lastdyn')

                dictionary['time'] = (datetime.utcfromtimestamp(dictionary['utc_timestamp'])+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

                if dictionary['type'] not in self.ship_type_dict:
                    sts_db_result = ShipTypeShipxy.query.filter(ShipTypeShipxy.type==dictionary['type']).first()
                    if sts_db_result:
                        self.ship_type_dict[sts_db_result.type] = sts_db_result.name
                    else:
                        self.ship_type_dict[dictionary['type']] = None
                dictionary['type_text'] = self.ship_type_dict[dictionary['type']]

                if dictionary['navistatus'] not in self.navistatus_type_dict:
                    nt_db_result = NavistatusTypeShipxy.query.filter(NavistatusTypeShipxy.type==dictionary['navistatus']).first()
                    if nt_db_result:
                        self.navistatus_type_dict[nt_db_result.type] = nt_db_result.name
                    else:
                        self.navistatus_type_dict[dictionary['navistatus']] = None
                dictionary['navistatus_text'] = self.navistatus_type_dict[dictionary['navistatus']]

                dictionary['_routing'] = '{}'.format((datetime.utcfromtimestamp(dictionary['utc_timestamp'])+timedelta(hours=8)).year)

                # dictionary['updatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # 這邊是如果字串後面有包含空格，就去除空格
                # 如果是空字串，就設為null
                for key in list(dictionary.keys()):
                    if type(dictionary[key]) is not str:
                        continue
                    dictionary[key] = dictionary[key].strip()
                    if not dictionary[key] or dictionary[key]=='NULL':
                        dictionary[key] = None

                batch_load_list.append(dictionary)
            if delete_list:
                # 因為可能三台機器爬到同區域，如果有其中一台有刪除資料，這邊 Delete 就會出錯
                try:
                    self.es.delete_data(delete_list)
                except:
                    pass
            if batch_load_list:
                self.es.batch_load(batch_load_list)
            del(delete_list, batch_load_list)
        except:
            msg_list = ['ip: {}'.format(get_external_ip()), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()]
            print('\n\n'.join(msg_list))
            ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(msg_list))
            ggg.send_email()

    def shipxy_crawler_func(self):
        try:
            ip = get_external_ip()
        except:
            ip = '取得IP失敗'
        try:
            # 檢查這台機器是否有同排程還在執行
            if check_same_process_still_running(self.script_name):
                # 代表包含這個程式在內，有兩個以上相同的排程正在運行
                print('{}: 有相同排程尚在執行({})'.format(self.script_name, 1))
                return
        except:
            msg = '\n\n'.join(['ip: {}'.format(ip), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()])
            print(msg)
            ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
            ggg.send_email()
            # line_notify_pusher(msg)
            return

        try:
            self.es = Elastic(
                host=app.config['ES_SETTING']['CONNECTION']['HOST'], 
                port=app.config['ES_SETTING']['CONNECTION']['PORT'], 
                username=app.config['ES_SETTING']['CONNECTION']['ACCOUNT'],
                password=app.config['ES_SETTING']['CONNECTION']['PASSWORD']
            )
            if not self.es.check_index_exist(app.config['ES_SETTING']['INDEX_INFO']['SHIPXY']['INDEX_NAME']):
                print(self.es.create_index(app.config['ES_SETTING']['INDEX_INFO']['SHIPXY']['INDEX_NAME'], app.config['ES_SETTING']['INDEX_INFO']['SHIPXY']['MAPPING_FILEPATH']))

            db_result_list = AreaList.query.with_entities(AreaList.id, AreaList.crawl_span).order_by(asc(AreaList.crawl_span)).all()
            if not db_result_list:
                print('{}: 無區域的排程區間資料'.format(self.script_name))
                return
            crawl_span_dict = {db_result.id:db_result.crawl_span for db_result in db_result_list}
            query_sort_conds = [SubAreaList.area_list_id]
            query_sort_conds.extend([x.id for x in db_result_list])

            self.cold_zone_ids = set([db_result.id for db_result in AreaList.query.filter(AreaList.enable==1, AreaList.name.like('%冷區%')).all()])

            self.mmsi_dict = {}
            for db_result in MMSI_Info.query.with_entities(MMSI_Info.mmsi, MMSI_Info.alpha_2, MMSI_Info.alpha_3).all():
                self.mmsi_dict[db_result.mmsi] = db_result.alpha_3 if db_result.alpha_3 else db_result.alpha_2

            self.sc = ShipXY_Crawler()

            cookies_list = []
            for db_result in ShipxyAccount.query.filter(ShipxyAccount.enable==1, 
                                                        ShipxyAccount.updating==0,
                                                        ShipxyAccount.updated_time>=(datetime.now()-timedelta(days=1))
                                                        ).all():
                if not db_result.cookies:
                    continue
                cookies_list.append(deepcopy(db_result.cookies))
            if not cookies_list:
                raise Exception('{}: 無可用之帳號'.format(self.script_name))
            self.sc.update_cookies_list(cookies_list)
            
            del(cookies_list)

            while True:
                if not self.crawler_status:
                    raise Exception('\n'.join(list(set(self.error_msg_list))))
                elif self.get_shipxy_thread_list and [x.is_alive() for x in self.get_shipxy_thread_list].count(True)>=self.get_shipxy_thread_limit_tmp:
                    continue
                remove_index_list = []
                for index, thread in enumerate(self.get_shipxy_thread_list):
                    if not thread.is_alive():
                        remove_index_list.append(index)
                remove_index_list.reverse()
                for index in remove_index_list:
                    del(self.get_shipxy_thread_list[index])

                cookies_list = []
                for cookies in self.sc.cookies_list:
                    if 'SERVERID' in cookies:
                        SERVERID_list = cookies['SERVERID'].split('|')
                        SERVERID_list[1] = '{}'.format(time.time())
                        SERVERID_list[2] = '{}'.format(time.time())
                        cookies['SERVERID'] = '|'.join(SERVERID_list)
                    cookies_list.append(cookies)
                self.sc.update_cookies_list(cookies_list)
                del(cookies_list)

                db_result = CrawlerMachine.query.filter(CrawlerMachine.ip==ip).first()
                if not db_result:
                    db_result = CrawlerMachine(ip=ip)
                db_result.updatedAt = datetime.now()
                db.session.add(db_result)
                db.session.commit()

                machine_quantity = CrawlerMachine.query.filter(CrawlerMachine.updatedAt>=(datetime.now()-timedelta(hours=1))).count()
                if not machine_quantity:
                    machine_quantity+=1
                # 每個帳號平均每秒只能查詢一次區域，以避免帳號被鎖
                # 算式為：(一秒/((可用帳號數量)/(機器總數)))-(這一輪當前經過的時間)
                self.gernal_sleep_time = (1/len(self.sc.cookies_list))*machine_quantity*1.5

                self.get_shipxy_thread_limit_tmp = (math.floor((len(self.sc.cookies_list)/machine_quantity)/self.get_ship_detail_quantity_limit))
                if not self.get_shipxy_thread_limit_tmp:
                    raise Exception(
                        '\n'.join(
                            [
                                '{}: 帳號總數量未達可爬取之帳號最小數量\n'.format(self.script_name),
                                '最小數量定義的算式為\n',
                                '可用帳號之數量({}) 除以 機器總數({}) 除以 每個 thread 取得船隻詳細資料的子程序上限值({}) 後取最小值整數'.format(len(self.sc.cookies_list), machine_quantity, self.get_ship_detail_quantity_limit)
                            ]
                        )
                    )

                if self.db_rollback_dict:
                    for db_result_id in list(self.db_rollback_dict.keys()):
                        db_result = SubAreaList.query.filter(SubAreaList.id==db_result_id).first()
                        db_result.crawler_time = self.db_rollback_dict[db_result_id]['crawler_time']
                        db_result.next_time = self.db_rollback_dict[db_result_id]['next_time']
                        db.session.add(db_result)
                        del(self.db_rollback_dict[db_result_id])
                    db.session.commit()

                db_result = SubAreaList.query.filter(
                        SubAreaList.enable==1, 
                        SubAreaList.web=='shipxy',
                        or_(SubAreaList.next_time<=datetime.now(), 
                        SubAreaList.next_time==None), 
                        or_(*[SubAreaList.area_list_id==id for id in crawl_span_dict.keys()])
                    ).order_by(
                        sqlalchemy.func.field(*query_sort_conds), 
                        asc(SubAreaList.next_time),
                        func.random()
                    ).first()

                if not db_result:
                    if [x.is_alive() for x in self.get_shipxy_thread_list].count(True):
                        print('{}: 無需要爬取的區域, 等待仍在執行的的區域爬取子程序結束中，如果所有子程序執行結束且無任何需爬取的區域，程式將會結束'.format(self.script_name))
                        while [x.is_alive() for x in self.get_shipxy_thread_list].count(True):
                            # 如果到有區域需要再爬的時間，就繼續爬區域
                            if not datetime.now().minute \
                            or datetime.now().minute in crawl_span_dict.values():
                                break
                        # 這邊寫 continue 不是 return，是因為如果子程序執行結束，時間剛好過30分或是0分，就會又有區域需要爬
                        continue
                    else:
                        print('{}: 無需要爬取的區域, 程式結束, 時間: {}'.format(self.script_name, datetime.now()))
                    return

                get_shipxy_thread_input = deepcopy(db_result.json())

                if db_result.area_list_id not in crawl_span_dict:
                    crawl_span_dict[db_result.area_list_id] = AreaList.query.filter(AreaList.id==db_result.area_list_id).first().crawl_span
                crawler_time = datetime.now()-timedelta(minutes=datetime.now().minute%crawl_span_dict[db_result.area_list_id])
                db_result.crawler_time = datetime.strptime(crawler_time.strftime('%Y-%m-%d %H:%M:00'), '%Y-%m-%d %H:%M:%S')
                db_result.next_time = db_result.crawler_time+timedelta(minutes=crawl_span_dict[db_result.area_list_id])
                db.session.add(db_result)
                db.session.commit()

                if db_result.lu_lat==db_result.rd_lat \
                or db_result.lu_lng==db_result.rd_lng:
                    continue

                db.session.rollback()
                db.session.close()


                thread = threading.Thread(target=self.get_shipxy_thread, args=(get_shipxy_thread_input, ), daemon=True)
                thread.start()
                self.get_shipxy_thread_list.append(thread)

                # ###############################
                # for thread in self.get_shipxy_thread_list:
                #     thread.join()
                # return
                # ###############################
        except:
            msg = '\n\n'.join(['ip: {}'.format(ip), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()])
            print(msg)
            ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(self.script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
            ggg.send_email()
            # line_notify_pusher(msg)

if __name__ == "__main__":
    scc = shipxy_crawler_class()
    scc.shipxy_crawler_func()