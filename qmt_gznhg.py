#coding:gbk

import json


import datetime
from datetime import datetime as dt

from aare.noqa import *


class SUB:
    pass


nhg_1d_sh = '204001.SH'
nhg_1d_sz = '131810.SZ'


qmt_cookie = '230041917'


time_now = dt.now()
today_str = datetime.datetime.now().strftime("%Y%m%d")
trade_day = datetime.datetime.now().date().strftime('%Y%m%d')
trade_start = dt.strptime(time_now.strftime("%Y-%m-%d 09:16:00"), "%Y-%m-%d %H:%M:%S")


def init(ct):
    ct.set_account(qmt_cookie)
    ct.run_time("process_gz_nhg", '1nDay', "2023-01-01 15:03:00")
    print("GZ nhg start")


def process_gz_nhg(ct):
    print('in timer')
    cash = get_trade_detail_data(qmt_cookie, 'stock', 'account')[0].m_dAvailable
    vol = int(cash / 1000) * 10
    print("可用余额：", cash)
    print("数量：", vol)

    if vol >= 1000:
        ret = ct.get_market_data(['quoter'], stock_code=[nhg_1d_sh], start_time='', end_time='', period='tick')
        buy5_price = ret['bidPrice'][-1]
        lastPrice = ret['lastPrice']
        print(buy5_price, lastPrice)
        
        passorder(24, 1101, qmt_cookie, nhg_1d_sh, 11, buy5_price, vol, '', 2, '', ct)
    else:
        print('可用余额不足')


def handlebar(ct):

    return


def deal_callback(ct, dealInfo):
    print(f'>>>NHG dealInfo : {dealInfo.m_strInstrumentID}, {dealInfo.m_nRef}')


def order_callback(ct, orderInfo):
    print(f'>>>NHG  orderInfo : {orderInfo.m_strInstrumentID},{orderInfo.m_nRef}')
