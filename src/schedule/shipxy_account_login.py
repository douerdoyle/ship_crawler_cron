import sys
if __name__=='__main__':
    sys.path.append('../')
import traceback, requests, os, json
from datetime          import datetime, timedelta
from copy              import deepcopy
from pprint            import pprint
from sqlalchemy        import asc, or_, func
from lib.tools         import ShipXY_Crawler, check_same_process_still_running, get_external_ip
from lib.send_email.gmail_sender import GmailSender
from settings.environment import db, app
from models.account import ShipxyAccount

def ship_account_login_func():
    script_name = os.path.basename(__file__)
    try:
        # 檢查這台機器是否有同排程還在執行
        if check_same_process_still_running(script_name):
            # 代表包含這個程式在內，有兩個ca以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(script_name, 1))
            return

        # {"SERVERID": "ce54c768aca7be22386d8a7ce24ecdae|1596424213|1596424207", ".UserAuth2": "DC41F8152480DF00C47C6EA6666546EFD31E197BB9DCD08A6D47A60FE95B8D15A5963DA3196A79A31CD4DCABD9D15BC24D1D3B6AA57B108A9BF1C4350DAA0A12D2352E089006D4B5B285875C837BBC8A26E5069E1657CE48636716B1A820826E4AA7D4DF86AC7AA714B37C615B2A49AC245CB0FFEC011405D9F3F22085AC55D998184EF5", "FD857C2AF68165D4": "vyH3H7apfudRFtw8Dvd2z9dXvgh6/nhEsXkCr3rtsqflVSiS4EwmTNvclp+SBvVC", "ASP.NET_SessionId": "rp0u0ognzh3qzci3mnfdvjoi"}

        sc = ShipXY_Crawler()
        conds_list = [
            # 登入新加入的帳號
            [
                ShipxyAccount.enable==None
            ],

            # 如果有任何帳號更新到一半，重新登入的排程crash掉，會導致該筆帳號資料 updating 會保持在1，故再重登更新失敗的帳號一次
            [
                ShipxyAccount.updating==1, 
                ShipxyAccount.updated_time<=(datetime.now()-timedelta(minutes=30))
            ],

            # 重登登入超過23小時的帳號，由於船訊網爬蟲會撈取一天內曾登入的帳號，故最晚要在22小時更新
            [
                ShipxyAccount.enable==1, 
                ShipxyAccount.updating==0,
                or_(
                    ShipxyAccount.updated_time<=(datetime.now()-timedelta(hours=22)),
                    ShipxyAccount.updated_time==None
                )
            ]
        ]
        account_be_banned = []
        for conds in conds_list:
            while True:
                db.session.rollback()
                db.session.close()
                db_result = ShipxyAccount.query.filter(*conds).order_by(func.random()).first()
                if not db_result:
                    break
                db_result.updating = 1
                db.session.add(db_result)
                db.session.commit()

                db_result.updating = 0
                db_result.cookies = sc.login(db_result.account, db_result.password, db_result.cookies)
                if '.UserAuth2' not in db_result.cookies or 'FD857C2AF68165D4' not in db_result.cookies:
                    db_result.enable = 0
                    account_be_banned.append(db_result.account)
                else:
                    db_result.enable = 1

                db.session.add(db_result)
                db.session.commit()
            account_be_banned = list(set(account_be_banned))
            account_be_banned.sort()
            if account_be_banned:
                msg_list = [
                    'ip: {}'.format(get_external_ip()), 
                    '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                    '有帳號被封鎖，下述為被封鎖之帳號',
                    '\n'.join(account_be_banned)
                    ]
                ggg = GmailSender('船隻爬蟲狀況通知-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(msg_list))
                ggg.send_email()
    except:
        msg_list = ['ip: {}'.format(get_external_ip()), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()]
        print('\n\n'.join(msg_list))
        ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(msg_list))
        ggg.send_email()

if __name__=='__main__':
    ship_account_login_func()