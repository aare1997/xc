# coding:gbk

import json
import queue
import pandas as pd
import threading
import QUANTAXIS as QA
import pymongo
import time
import datetime
from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher, publisher_topic
from aare.noqa import *
from aare.qmt2qifi import *


mongo_ip = '127.0.0.1'
eventmq_ip = '127.0.0.1'

qmt_cookie = '230041917'
XC_PREFIX = 'xcqmt_acc_'
QMT_ACC = XC_PREFIX + qmt_cookie
QMT_POSITIONS = XC_PREFIX + qmt_cookie + '_positions'
QMT_ORDERCB = XC_PREFIX + qmt_cookie + '_ordercb'
QMT_ORDER = XC_PREFIX + qmt_cookie + '_order'
QMT_IPO = XC_PREFIX + qmt_cookie + '_ipo'
QMT_LOG = XC_PREFIX + qmt_cookie + '_log'


class SUB:
    pass


log_info = SUB()
log_info.positioncb_count = 0
log_info.accountcb_count = 0
log_info.ordercb_count = 0
log_info.dealcb_count = 0


database = pymongo.MongoClient(mongo_ip).QMTREALTIME
pro = publisher_topic(exchange='QAQMTGateway', routing_key=qmt_cookie, host=eventmq_ip)
holding = {}

orderq = queue.Queue()
available_rq = {}
available_holding = {}


def init(ct):
    ct.set_account(qmt_cookie)
    sub = subscriber_routing(exchange='QAQMTORDER', routing_key=qmt_cookie, host=eventmq_ip)
    sub.callback = qaorderrouter

    threading.Thread(target=sub.start, daemon=True).start()
    ct.run_time("acc_timer_run", "10nSecond", "2019-10-14 13:20:00")
    ct.run_time("ipo_timer_run", "1nDay", "2023-01-01 14:35:00")
    ct.run_time("repo_timer_run", "1nDay", "2023-02-01 15:08:00")


def acc_timer_run(ct):
    order = rdj_queue_pop(QMT_ORDER)
    if order:
        print(f'in timer :{order}')
        dispatch_order(ct, order)

    output_acc(qmt_cookie)
    output_pos(qmt_cookie)
    ipo_info(ct)


def repo_timer_run(ct):
    repo_1d_sh = '204001.SH'
    repo_1d_sz = '131810.SZ'

    if datetime.datetime.now().weekday() != 3:
        print(f'不是星期四，不做逆回购')
        return

    cash = get_trade_detail_data(qmt_cookie, 'stock', 'account')[0].m_dAvailable
    vol = int(cash / 1000) * 10
    print("可用余额：", cash, "   数量：", vol)

    if vol >= 1000:
        ret = ct.get_market_data(['quoter'], stock_code=[repo_1d_sh], start_time='', end_time='', period='tick')
        buy5_price = ret['bidPrice'][-1]
        passorder(24, 1101, qmt_cookie, repo_1d_sh, 11, buy5_price, vol, '', 2, '', ct)
        print(f'buy {repo_1d_sh} {buy5_price}, vol {vol}')

    elif vol > 10:
        ret = ct.get_market_data(['quoter'], stock_code=[repo_1d_sz], start_time='', end_time='', period='tick')
        buy5_price = ret['bidPrice'][-1]
        passorder(24, 1101, qmt_cookie, repo_1d_sz, 11, buy5_price, vol, '', 2, '', ct)
        print(f'buy {repo_1d_sz} {buy5_price}, vol {vol}')
    else:
        print('可用余额不足{cash}, 没有逆回购!')


def ipo_timer_run(ct):
    print(f'ipo timer run')

    pass


def output_acc(cookie):
    accounts = get_trade_detail_data(cookie, 'stock', 'account')
    if accounts:
        px = unpack_data(accounts[0])
        rdj_set(QMT_ACC, json.dumps(px, cls=Py36JsonEncoder))
    else:
        print('No account find!')


def output_pos(cookie):
    positions = get_trade_detail_data(cookie, 'stock', 'position')
    pos_list = []
    for dt in positions:
        px = unpack_data(dt, not_use=['m_dStockLastPrice', 'm_dStaticHoldMargin'])
        pos_list.append(px)

    if pos_list:
        rdj_set(QMT_POSITIONS, json.dumps(pos_list, cls=Py36JsonEncoder))


def output_cancel(ct):
    orders = get_cancel_list(ct)
    cancel_list = []
    for item in orders:
        px = unpack_data(item, not_use=['m_dShortOccupedMargin'])
        cancel_list.append(px)

    if cancel_list:
        rdj_queue_push(QMT_ORDERCB, json.dumps({"topic": "cancel_list", "data": cancel_list}, cls=Py36JsonEncoder))


def ipo_info(ct):
    ipo = get_ipo_data()  # "STOCK"新股信息, "BOND" 返回新债申购信息,"" ALL
    if ipo:
        limit = get_new_purchase_limit(qmt_cookie)
        ipo['limit'] = limit
        rdj_set(QMT_IPO, json.dumps(ipo, cls=Py36JsonEncoder))


def get_cancel_list(ct):
    orders = get_trade_detail_data(qmt_cookie, 'stock', 'order')
    can_cancel = [order for order in orders if order.m_nOrderStatus in [48, 49, 50, 55]]
    return can_cancel


def cancel_some(ct, order=None):
    if order:
        if order['order_id'] != 0:
            cancel(order['order_id'], qmt_cookie, 'stock', ct)
            return
        if order['cancel_all'] == True:
            can_cancel = get_cancel_list(ct)
            if can_cancel:
                for x in can_cancel:
                    print(f'in cancel all id : {x.m_strOrderSysID}')
                    cancel(x.m_strOrderSysID, qmt_cookie, 'stock', ct)
            return

        if len(order['code']) == 6:
            print(f'cancel by code {order["code"]} no imp now!')


def one_order(ct, order):
    buy_sell = 24  # sell
    if order['direction'] == 'BUY':
        buy_sell = 23
    code = order['code']
    price_type = 11  # -1,无效， 5，最新价，11， 11：（指定价）模型价（只对单股情况支持,对组合交易不支持）
    quick_trade = 2
    passorder(
        buy_sell,
        1101,
        qmt_cookie,
        code,
        price_type,
        order['price'],
        order['volume'],
        order['strategy_id'],
        quick_trade,
        'aare',
        ct,
    )

    print(buy_sell, order['code'], order['price'], order['volume'], order['strategy_id'], quick_trade)
    msg = json.dumps(order)
    rdj_queue_push(QMT_LOG, msg)


def dispatch_order(ct, order):
    if isinstance(order, str):
        print(f'order is str？ : {order} ')
        return
    if order['topic'] in ['insert_order', 'manual_order']:
        one_order(ct, order)

    elif order['topic'] == 'cancel_order':
        cancel_some(ct, order)

    elif order['topic'] == 'cancel_list':
        output_cancel(ct)


def handlebar(ct):
    order = rdj_queue_pop(QMT_ORDER)
    if order:
        print(f'in handlebar : {order}')
        dispatch_order(ct, order)

    for x in range(orderq.qsize()):
        try:
            r = orderq.get_nowait()
            if r['topic'] == 'insert_order':
                one_order(ct, r)

        except Exception as r:
            traceback.print_exc()


def qaorderrouter(ct, a, b, data):
    try:
        r = json.loads(data)
        orderq.put_nowait(r)
    except:
        print('qaorderrouter error!')
        pass


def unpack_data(h, not_use=None):
    key = [r for r in dir(h) if r[0] != '_']
    if not_use:
        # key=list(set(key)-set(['m_dStockLastPrice','m_dStaticHoldMargin']))
        key = list(set(key) - set(not_use))
    value = []
    keys = []
    for i in key:
        if i.startswith('m_'):
            try:
                value.append(eval("h.{}".format(i)))
                keys.append(i)
            except:
                pass

    px = dict(zip(keys, value))
    px['account_cookie'] = qmt_cookie
    return px


def pub_msg(ct, h, topics):
    px = unpack_data(h)
    pro.pub(json.dumps({'topic': topics, 'data': px}), routing_key=qmt_cookie)


def account_callback(ct, accountInfo):
    log_info.accountcb_count += 1

    output_acc(qmt_cookie)
    pub_msg(ct, accountInfo, 'account')
    if log_info.accountcb_count % 10 == 0:
        print(f'Gateway : acccb {log_info.accountcb_count} ,  poscb {log_info.positioncb_count} ')


def position_callback(ct, positonInfo):
    pub_msg(ct, positonInfo, 'position')
    output_pos(qmt_cookie)
    log_info.positioncb_count += 1
    if log_info.positioncb_count % 10 == 0:
        print(f'Gateway : poscb {log_info.positioncb_count}, acccb {log_info.accountcb_count}')


def order_callback(ct, orderInfo):
    print(f'orderInfo : {orderInfo.m_strInstrumentID}')

    data = unpack_data(orderInfo)
    msg = json.dumps({"topic": "orderInfo", "data": data}, cls=Py36JsonEncoder)
    rdj_queue_push(QMT_ORDERCB, msg)
    rdj_queue_push(QMT_LOG, msg)
    pub_msg(ct, orderInfo, 'order')
    log_info.ordercb_count += 1


def deal_callback(ct, dealInfo):
    print(f'dealInfo : {dealInfo.m_strInstrumentID}')

    data = unpack_data(dealInfo)
    msg = json.dumps({"topic": "dealInfo", "data": data}, cls=Py36JsonEncoder)
    rdj_queue_push(QMT_ORDERCB, msg)
    rdj_queue_push(QMT_LOG, msg)
    pub_msg(ct, dealInfo, 'trade')
    log_info.dealcb_count += 1


def orderError_callback(ct, passOrderInfo, msg):
    print('orderError_callback')

    data = unpack_data(passOrderInfo)
    jmsg = json.dumps({"topic": "orderError", "data": data, "msg": msg}, cls=Py36JsonEncoder)
    rdj_queue_push(QMT_ORDERCB, jmsg)
    rdj_queue_push(QMT_LOG, jmsg)
    pub_msg(ct, passOrderInfo, 'error_order')
