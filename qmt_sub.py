#encoding:gbk
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

log_info=SUB()
log_info.sub_code=0
log_info.sub_code_total = 0
log_info.sub_event_count = 0

mongo_ip = '127.0.0.1'
eventmq_ip = '127.0.0.1'
account_cookie = '230041917'


time_now = dt.now()
today_str = datetime.datetime.now().strftime("%Y%m%d")
trade_day=datetime.datetime.now().date().strftime('%Y%m%d')
trade_start = dt.strptime(time_now.strftime("%Y-%m-%d 09:16:00"), "%Y-%m-%d %H:%M:%S")
trade_open_stop = dt.strptime(time_now.strftime("%Y-%m-%d 09:25:25"), "%Y-%m-%d %H:%M:%S")
trade_mid_stop = dt.strptime(time_now.strftime("%Y-%m-%d 11:30:05"), "%Y-%m-%d %H:%M:%S")
trade_mid_start = dt.strptime(time_now.strftime("%Y-%m-%d 13:00:00"), "%Y-%m-%d %H:%M:%S")
trade_end = dt.strptime(time_now.strftime("%Y-%m-%d 15:01:00"), "%Y-%m-%d %H:%M:%S")

snapshot_open_pub = publisher_topic(exchange='qmt_stock_snapshot_open', routing_key='', host=eventmq_ip)
amx_tick_pub = publisher_topic(exchange='qmt_stock_amx_tick', routing_key='', host=eventmq_ip)


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

    # sub_param.full_code = ct.get_stock_list_in_sector('沪深A股')

    if dt.now() < trade_open_stop:
        ct.run_time("qmt_timer_run", "3nSecond", "2019-10-14 13:20:00")
        param.open_time_run = False
    else:
        param.open_time_run = False
        ct.run_time("qmt_timer_run", "3nSecond", "2019-10-14 13:20:00")


    param.ct = ct




def qmt_timer_run(ct):
    if param.open_time_run and dt.now() > trade_start:

        full_tick = ct.get_full_tick(param.code_nost)
        snapshot_open_pub.pub(
            json.dumps({'topic': 'open', 'data': full_tick}, cls=Py36JsonEncoder), routing_key='open'
        )
        print(f'open time pub: {len(full_tick)}')
        if dt.now() > trade_open_stop:
            param.open_time_run = False
            
    event = has_qmt_evnt()

    if event:
        try:
            if event['control'] == 'get_tick':
                if param.tick_sub_list:
                    param.tick_sub_list.extend(event['code'])
                else:
                    param.tick_sub_list = event['code']
                param.tick_sub_day=trade_day
                if 'date' in event:
                    param.tick_sub_day=event['date']
                log_info.sub_code = len(param.tick_sub_list)
                log_info.sub_event_count +=1
                print(f'Sub code {log_info.sub_code}, sub count : {log_info.sub_event_count }')
                
        
                tick_get_ex_ori()
            elif event['control'] == 'get_min':
                df = get_min_nday(event['code'], '20220930')
                print(df)
                df = df.reset_index()
                if not df.empty:
                    print(f'will pub min:{event["code"]}')
                    amx_tick_pub.pub(
                        json.dumps({'topic': "tick", 'data': df.to_json(orient='records')}, cls=Py36JsonEncoder),
                        routing_key='min',
                    )
            else:
                print(f'no support: {event}')
        except Exception as e:
            print(f"error event: {e}, {e.__str__()}")

    elif param.tick_sub_list:
        tick_get_ex_ori()

    return


def get_min_nday(codelist, start, type='1m'):
    for code in codelist:
        print(f'code: {code}')
        df = param.ct.get_market_data(
            ['open', 'high', 'low', 'close', 'volume', 'amount'],
            stock_code=[code],
            start_time=start,
            end_time='',
            dividend_type='front',
            period=type,
        )
        return df


def get_tick_nday(codelist, start_day):
    df = param.ct.get_market_data(['quoter'], stock_code=codelist, start_time=start_day, period='tick')
    return df


def tick_sub_list_run_old():
    max_idx = min(3, len(param.tick_sub_list))

    code = param.tick_sub_list[:max_idx]
    df = param.ct.get_market_data_dict(['quoter'], stock_code=code, start_time=param.tick_sub_day, period='tick')

    if not df.empty:
        amx_tick_pub.pub(
            json.dumps(
                {'topic': "tick", "code": code[:6], "format": "data_df", 'data': df.to_json()}, cls=Py36JsonEncoder
            ),
            routing_key='tick',
        )

    else:
        print(f'code no tick data: {code}')
    del param.tick_sub_list[:max_idx]


def tick_get_ori():
    max_idx = min(5, len(param.tick_sub_list))

    code = param.tick_sub_list[:max_idx]
    xx = param.ct.get_market_data_ori(['quoter'], stock_code=code, start_time=param.tick_sub_day, period='tick')

    for k in xx.keys():
        amx_tick_pub.pub(
            json.dumps({'topic': "tick", "code": k[:6], "format": "data_ori", 'data': xx[k]}, cls=Py36JsonEncoder),
            routing_key='tick',
        )

    gc.collect()
    del param.tick_sub_list[:max_idx]


def tick_get_ex():
    max_idx = min(10, len(param.tick_sub_list))

    code = param.tick_sub_list[:max_idx]
    df = param.ct.get_market_data_ex(fields=[], stock_code=code, period='tick', start_time=param.tick_sub_day, subscribe=True)
    for k in df.keys():
        amx_tick_pub.pub(
            json.dumps(
                {'topic': "tick", "code": k[:6], "format": "data_ex", 'data': df[k].to_json()}, cls=Py36JsonEncoder
            ),
            routing_key='tick',
        )

    del param.tick_sub_list[:max_idx]


def tick_get_ex_ori():

    max_idx = min(3, len(param.tick_sub_list))
    code = param.tick_sub_list[:max_idx]
    df = param.ct.get_market_data_ex_ori(
        fields=[], stock_code=code, period='tick', start_time=param.tick_sub_day, subscribe=True
    )
    
    for k in df.keys():
        del df[k]['pvolume'],df[k]['lastSettlementPrice'],df[k]['settlementPrice'],df[k]['transactionNum'],df[k]['openInt']
        del df[k]['stime'],df[k]['low'],df[k]['stockStatus']
        amx_tick_pub.pub(
            json.dumps({'topic': "tick", "code": k[:6], "format": "data_ex_ori", 'data': df[k]}, cls=Py36JsonEncoder),
            routing_key='tick',
        )

    del param.tick_sub_list[:max_idx]


def handlebar(ct):

    return


def control_qmt_cb(ct, a, b, data):
    try:

        r = json.loads(data)
        print(r)

    except:
        pass
