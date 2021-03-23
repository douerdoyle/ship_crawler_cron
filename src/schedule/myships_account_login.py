import sys
if __name__=='__main__':
    sys.path.append('../')
import traceback, requests, os, json, time
from datetime          import datetime, timedelta
from copy              import deepcopy
from pprint            import pprint
from sqlalchemy        import asc, or_, func
from execjs            import compile as execjs_compile
from models.account    import MyshipsAccount
from lib.tools         import check_same_process_still_running, get_external_ip
from lib.send_email.gmail_sender import GmailSender
from settings.environment import app, db

# MyshipsAccount.query.update({'enable':0, 'updating':0})
# db.session.commit()

def ship_account_login_func():
    script_name = os.path.basename(__file__)
    try:
        # 檢查這台機器是否有同排程還在執行
        if check_same_process_still_running(script_name):
            # 代表包含這個程式在內，有兩個ca以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(script_name, 1))
            return

        # 預防當要登入的帳號只有一個，但不同主機搶同一帳號登入
        try:
            time.sleep(os.environ.get('SERIAL'))
        except:
            pass

        f = open(app.config['CRAWLER_SETTING']['MYSHIPS']['JS_DEMIX_FILEPATH'], 'r')
        js_content = f.read()
        f.close()

        conds_list = [
            # 登入新加入的帳號
            [
                MyshipsAccount.enable==None
            ],

            # 如果有任何帳號更新到一半，重新登入的排程crash掉，會導致該筆帳號資料 updating 會保持在1，故再重登更新失敗的帳號一次
            [
                MyshipsAccount.updating==1, 
                MyshipsAccount.updated_time<=(datetime.now()-timedelta(minutes=30))
            ],

            # 重登登入超過23小時的帳號，由於船訊網爬蟲會撈取一天內曾登入的帳號，故最晚要在22小時更新
            [
                MyshipsAccount.enable==1, 
                MyshipsAccount.updating==0,
                or_(
                    MyshipsAccount.updated_time<=(datetime.now()-timedelta(minutes=30)),
                    MyshipsAccount.updated_time==None
                )
            ]
        ]
        headers = {
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
            'Connection':'keep-alive'
        }
        err_msg_list = []
        account_be_banned = []
        for conds in conds_list:
            while True:
                db.session.rollback()
                db.session.close()
                db_result = MyshipsAccount.query.filter(*conds).order_by(func.random()).first()
                if not db_result:
                    break
                db_result.updating = 1
                db.session.add(db_result)
                db.session.commit()

                db_result.updating = 0
                session = requests.Session()

                complie_result = execjs_compile(js_content).call('shipencode') # {'time': 1607309565213, 'pwdSign': '50aa7d92baa11df578ad254e252928c8'}
                complie_result['user'] = db_result.account

                try:
                    rsp = session.post(app.config['CRAWLER_SETTING']['MYSHIPS']['LOGIN'], headers=headers, json=complie_result, timeout=180)
                    rsp.close()
                except:
                    raise Exception(traceback.format_exc())
                try:
                    rsp_result = rsp.json()
                except:
                    raise Exception(rsp.text)
                if rsp_result['code']=='0':
                    pprint(rsp_result)
                    err_msg_list.append(f"{rsp_result}")
                    db_result.cookies = deepcopy(session.cookies.get_dict())
                    db_result.enable = 1
                else:
                    db_result.enable = 0
                    account_be_banned.append(db_result.account)
                db.session.add(db_result)
                db.session.commit()
        account_be_banned = list(set(account_be_banned))
        account_be_banned.sort()
        if account_be_banned:
            msg_list = [
                'ip: {}'.format(get_external_ip()), 
                '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), 
                '有帳號被封鎖，下述為被封鎖之帳號',
                '\n'.join(account_be_banned),
                '\n'.join(list(set(err_msg_list)))
                ]
            msg = '\n\n'.join(msg_list)
            print(msg)
            ggg = GmailSender('船隻爬蟲狀況通知-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], msg)
            ggg.send_email()
    except:
        msg_list = ['ip: {}'.format(get_external_ip()), '時間: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), traceback.format_exc()]
        print('\n\n'.join(msg_list))
        ggg = GmailSender('船隻爬蟲出現錯誤-{}'.format(script_name), app.config['GOOGLE_SENDER_CONF']['TO_LIST'], '\n\n'.join(msg_list))
        ggg.send_email()

if __name__=='__main__':
    ship_account_login_func()