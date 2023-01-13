# encoding:gbk
'''

'''
import pandas as pd
import numpy as np
import json
import queue
import pymongo
import time
import datetime
from datetime import datetime as dt
import threading
from aare.noqa import *
from pathlib import Path
import gc
import copy
from QAPUBSUB.consumer import subscriber, subscriber_topic, subscriber_routing
from QAPUBSUB.producer import publisher, publisher_topic


class SUB:
    pass


param = SUB()
param.tick_sub_list = []

log_info = SUB()
log_info.ticks_count = 0
log_info.ticks_loop_count = 0
log_info.ticks_last_time = 0
log_info.ticks_last_count = 0


mongo_ip = '127.0.0.1'
eventmq_ip = '127.0.0.1'
account_cookie = '230041917'


time_now = dt.now()
today_str = datetime.datetime.now().strftime("%Y%m%d")
trade_day = datetime.datetime.now().date().strftime('%Y%m%d')
trade_start = dt.strptime(time_now.strftime("%Y-%m-%d 09:16:00"), "%Y-%m-%d %H:%M:%S")
trade_open_stop = dt.strptime(time_now.strftime("%Y-%m-%d 09:25:25"), "%Y-%m-%d %H:%M:%S")
trade_mid_stop = dt.strptime(time_now.strftime("%Y-%m-%d 11:30:05"), "%Y-%m-%d %H:%M:%S")
trade_mid_start = dt.strptime(time_now.strftime("%Y-%m-%d 13:00:00"), "%Y-%m-%d %H:%M:%S")
trade_end = dt.strptime(time_now.strftime("%Y-%m-%d 15:01:00"), "%Y-%m-%d %H:%M:%S")

snapshot_pub = publisher_topic(exchange='qmt_stock_snapshot', routing_key='', host=eventmq_ip)
# snapshot_open_pub = publisher_topic(exchange='qmt_stock_snapshot_open', routing_key='', host=eventmq_ip)
# amx_tick_pub = publisher_topic(exchange='qmt_stock_amx_tick', routing_key='', host=eventmq_ip)


def stock_trading_time():
    now = datetime.datetime.now()
    if now < trade_start or now < trade_end:
        return False

    if now > trade_mid_stop and now < trade_mid_start:
        return False
    return True


def stock_open_time():
    now = datetime.datetime.now()
    if now > trade_start and now < trade_open_stop:
        return True
    else:
        return False


def init(ct):
    ct.set_account(account_cookie)
    sub = subscriber_routing(exchange='control_to_qmt', routing_key='', host=eventmq_ip)
    sub.callback = control_qmt_cb
    threading.Thread(target=sub.start, daemon=True).start()

    param.code_nost = code_list_to_qmt(jl_read('code_nost'))
    param.code_amx = code_list_to_qmt(jl_read('code_amx'))

    ct.subscribe_whole_quote(param.code_nost, full_quote_cb)
    param.ct = ct


def full_quote_cb(data):

    snapshot_pub.pub(json.dumps({'topic': 'quote', 'data': data}, cls=Py36JsonEncoder), routing_key='full')

    log_info.ticks_count += len(data)
    log_info.ticks_loop_count += 1
    if log_info.ticks_loop_count % 500 == 0:
        this_count = log_info.ticks_count - log_info.ticks_last_count
        tnow = time.time()
        tsec = tnow - log_info.ticks_last_time
        speed = int(this_count / tsec)
        print(
            f'Ticks loop {log_info.ticks_loop_count}, ticks : {log_info.ticks_count }, times : {tsec:.2f} speed : {speed}'
        )
        log_info.ticks_last_time = tnow
        log_info.ticks_last_count = log_info.ticks_count


def handlebar(ct):

    return


def control_qmt_cb(ct, a, b, data):
    try:

        r = json.loads(data)
        print(r)

    except:
        pass
