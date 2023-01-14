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

qmt_cookie='230041917'
XC_PREFIX='xcqmt_acc_'
QMT_ACC = XC_PREFIX + qmt_cookie
QMT_POSITIONS = XC_PREFIX + qmt_cookie + '_positions'
QMT_ORDERCB = XC_PREFIX + qmt_cookie + '_ordercb'
QMT_ORDER = XC_PREFIX + qmt_cookie + '_order'



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
    


def acc_timer_run(ct):
    print('timer run')
    ipo_info(ct)
    order_str = rdj_queue_pop(QMT_ORDER)

    if order_str:
        order = json.loads(order_str)
        print(order)
        one_order(ct, order)
    #output_acc(qmt_cookie)
    #output_pos(qmt_cookie)
    

def output_acc(cookie):
    accounts = get_trade_detail_data(cookie, 'stock', 'account')
    px=unpack_data(accounts[0])
    rdj_set(QMT_ACC, json.dumps(px,cls=Py36JsonEncoder))
    
def output_pos(cookie):
    positions = get_trade_detail_data(cookie, 'stock', 'position')
    pos_list = []
    for dt in positions:
        px = unpack_data(dt, not_use=['m_dStockLastPrice','m_dStaticHoldMargin'])
        pos_list.append(px)

    if pos_list:
        rdj_set(QMT_POSITIONS, json.dumps(pos_list,cls=Py36JsonEncoder))


def ipo_info(ct):
    ipo=get_ipo_data("STOCK")# 返回新股信息
    for x in ipo:
        print(x)
        limit=get_new_purchase_limit()




def passorderwithModel(ct, order):
    passorder(order['order_model'], 1101, qmt_cookie, order['code'], 5, -1, order['volume'], 'mymodel', 2, 'qagateway', ct)


def sell_normal(ct, order):
    passorder(34, 1101, qmt_cookie, order['code'], 6, -1, order['volume'], 'x', 2, 'qagateway', ct)




def buy_rz(ct, order):
    """
    passorder(opType, orderType, accountID, orderCode, prType, volume
    opType
            23：股票买入，或沪港通、深港通股票买入
            24：股票卖出，或沪港通、深港通股票卖出

            33：信用账号股票买入
            34：信用账号股票卖出
            70：专项融资买入
            71：专项融券卖出
            72：专项买券还券
            73：专项直接还券
            74：专项卖券还款
            75：专项直接还款

    orderType
            1101：单股、单账号、普通、股/手方式下单
            1102：单股、单账号、普通、金额（元）方式下单（只支持股票）
            1113：单股、单账号、总资产、比例 [0 ~ 1] 方式下单
            1123：单股、单账号、可用、比例[0 ~ 1]方式下单


    prType
            -1：无效（实际下单时,需要用交易面板交易函数那设定的选价类型）
            0：卖5价
            1：卖4价
            2：卖3价
            3：卖2价
            4：卖1价
            5：最新价
            6：买1价
            7：买2价（组合不支持）
            8：买3价（组合不支持）
            9：买4价（组合不支持）
            10：买5价（组合不支持）
            11：（指定价）模型价（只对单股情况支持,对组合交易不支持）
            12：涨跌停价
            13：挂单价
            14：对手价
            26：限价即时全部成交否则撤单(仅对股票期权申报有效)
            27：市价即成剩撤(仅对股票期权申报有效)
            28：市价即全成否则撤(仅对股票期权申报有效)
            29：市价剩转限价(仅对股票期权申报有效)
            42：最优五档即时成交剩余撤销申报(仅对上交所申报有效)
            43：最优五档即时成交剩转限价申报(仅对上交所申报有效)
            44：对手方最优价格委托(仅对深交所申报有效)
            45：本方最优价格委托(仅对深交所申报有效)
            46：即时成交剩余撤销委托(仅对深交所申报有效)
            47：最优五档即时成交剩余撤销委托(仅对深交所申报有效)
            48：全额成交或撤销委托(仅对深交所申报有效)
            49：盘后定价

    price
            一、单股下单时，prType 是模型价/科创板盘后定价时 price 有效；其它情况无效；即单股
    时， prType 参数为 11，49 时被使用。 prType 参数不为 11，49 时也需填写，填写的内容
    可为 -1，0，2，100 等任意数字；
    volume
    """
    passorder(27, 1101, qmt_cookie, order['code'], 4, -1,  order['volume'], 'x', 2, 'qagateway', ct)

def one_order(ct, order):
    buy_sell= 24 #sell
    if order['direction'] == 'BUY':
        buy_sell=23

    price_type = -1 #-1,无效， 5，最新价，11， 11：（指定价）模型价（只对单股情况支持,对组合交易不支持）
    quick_trade = 2
    print(buy_sell, order['code'], order['price'])
    passorder(buy_sell, 1101, qmt_cookie, order['code'], price_type, order['price'], 
                 order['volume'], order['strategy_id'], quick_trade, 'aare', ct)

       
def handlebar(ct):

    order_str = rdj_queue_pop(QMT_ORDER)
    if order_str:
        order = json.loads(order_str)
        print(order)
        one_order(ct, order)

    for x in range(orderq.qsize()):
        try:            
            r = orderq.get_nowait()
            if r['topic'] == 'insert_order':
                one_order(ct, r)
        
        except Exception as r:
            traceback.print_exc()

    

def handlebar_qa(ct):
    pos = get_trade_detail_data(qmt_cookie, 'credit', 'position')
    for positonInfo in pos:
        # print(positonInfo.m_strInstrumentID)
        # if positonInfo.m_strInstrumentID
        available_holding[positonInfo.m_strInstrumentID] = positonInfo.m_nCanUseVolume
        holding[positonInfo.m_strInstrumentID] = positonInfo.m_nCanUseVolume

    for x in range(orderq.qsize()):
        try:
            r = orderq.get_nowait()
            print('hold', holding.get(r['code'], 0))
            if r['topic'] == 'insert_order':
                # "price": 12.81, "order_direction": "SELL", "order_offset": "OPEN",
                if r.get('order_model', "AUTO") != "AUTO":
                    passorderwithModel(ct, r)
                else:

                    if r['order_direction'] == "BUY" and holding.get(r['code'], 0) >= r['volume']:
                        if r['code'] in ['600777', '603056', '603256', '603486', '000564', '000785', '002936', '002946']:

                            buy_normal(ct, r)
                        else:
                            buy_rz(ct, r)
                        """
						if r['order_offset'] == "OPEN":
							buy_rz(ct,r)
						elif r['order_offset'] == "CLOSE":
							holding = ct.holding.get(r['code'],None)
							if holding:
								if holding.m_nCanUseVolume
							buy_close_hq(ct,r)
						"""
                    else:
                        # print(holding)
                        if available_holding.get(r['code'], 0) >= r['volume']:
                            sell_normal(ct, r)

                        """
						if r['order_offset'] == "OPEN":
							sell_rq(ct,r)
						elif r['order_offset'] == "CLOSE":
							sell_close_hk(ct,r)
						"""
        except:
            pass

    orders = get_trade_detail_data(qmt_cookie, 'credit', 'order')
    #can_cancel = [order for order in orders if can_cancel_order(order.m_strOrderSysID, qmt_cookie, 'credit')]
    can_cancel = [        order for order in orders if order.m_nOrderStatus in [48, 49, 50, 55]]
    now = datetime.datetime.now()
    if is_trade_time(now):
        print('check order can cancel: ', len(can_cancel))
        for order in can_cancel:
            #o_seconds = int(order.m_strInsertTime[-2:])
            o_minute = int(order.m_strInsertTime[-4:-2])
            #o_hour =  int(order.m_strInsertTime[:-4])
            if now.minute - o_minute > 1:

                try:
                    if order.m_strOptName in ['担保品买入', '担保品卖出', '融资买入', '融券卖出']:
                        order_model = 33
                        if order.m_strOptName == '担保品买入':
                            order_model = 33
                        elif order.m_strOptName == '担保品卖出':
                            order_model = 34
                        elif order.m_strOptName == '融资买入':
                            order_model = 27
                        elif order.m_strOptName == '融券卖出':
                            order_model = 28

                        if cancel(order.m_strOrderSysID, qmt_cookie, 'credit', ct):
                            # m_nVolumeTotal  m_strOptName
                            if order_model in [27, 33]:
                                print(
                                    'pass buy order', order.m_strInstrumentID, order.m_nVolumeTotal)
                                passorder(order_model, 1101, qmt_cookie, order.m_strInstrumentID,
                                          4, -1, order.m_nVolumeTotal, 'x', 1, 'qagateway', ct)
                            else:
                                print(
                                    'pass sell order', order.m_strInstrumentID, order.m_nVolumeTotal)
                                passorder(order_model, 1101, qmt_cookie, order.m_strInstrumentID,
                                          6, -1, order.m_nVolumeTotal, 'x', 1, 'qagateway', ct)
                except Exception as r:
                    traceback.print_exc()


def is_trade_time(_time):
    if _time.hour in [10, 13, 14]:
        return True
    elif (
            _time.hour in [9] and _time.minute > 15
    ):  # 修改成9:15 加入 9:15-9:30的盘前竞价时间
        return True
    elif _time.hour in [11] and _time.minute < 30:
        return True
    else:
        return False


def qaorderrouter(ct, a, b, data):
    try:
        r = json.loads(data)
        orderq.put_nowait(r)

    except:
        print('qaorderrouter error!')
        pass

def unpack_data(h,not_use=None):
    key = [r for r in dir(h) if r[0] != '_']
    if not_use:
        #key=list(set(key)-set(['m_dStockLastPrice','m_dStaticHoldMargin']))
        key=list(set(key)-set(not_use))
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
    print('accountInfo')
    # 输出资金账号状态

    # print(accountInfo.m_strStatus)
    output_acc(qmt_cookie)
    pub_msg(ct, accountInfo, 'account')

    # available = []
    # obj_list = get_enable_short_contract(qmt_cookie)

    # for i in obj_list:
    #     pdata = unpack_data(i)
    #     available.append(pdata)

    # if len(available) > 0:
    #     database.available.drop()
    #     database.available.insert_many(available)
    



    #positions = get_trade_detail_data(qmt_cookie, 'stock', 'position')
    #for dt in positions:
    #    print(f'股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
    #    f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')



# 委托主推函数


def order_callback(ct, orderInfo):
    print('orderInfo')
    # 输出委托证券代码
    print(orderInfo.m_strInstrumentID)
    data=unpack_data(orderInfo)
    rdj_queue_push(QMT_ORDERCB, json.dumps({"topic":"orderInfo", "data":data}, cls=Py36JsonEncoder))
    pub_msg(ct, orderInfo, 'order')
# 成交主推函数


def deal_callback(ct, dealInfo):
    print('dealInfo')
    # 输出成交证券代码Info
    print(dealInfo.m_strInstrumentID)
    data=unpack_data(dealInfo)
    rdj_queue_push(QMT_ORDERCB, json.dumps({"topic":"dealInfo", "data":data}, cls=Py36JsonEncoder))
    pub_msg(ct, dealInfo, 'trade')
    
# 持仓主推函数


def position_callback(ct, positonInfo):
    print('positonInfo')
    # 输出持仓证券代码
    #print(dir(positonInfo))
    pub_msg(ct, positonInfo, 'position')
    output_pos(qmt_cookie)
    
# 下单出错回调函数


def orderError_callback(ct, passOrderInfo, msg):
    print('orderError_callback')
    # 输出下单信息以及错误信息
    print(passOrderInfo.orderCode)
    print(msg)
    data=unpack_data(passOrderInfo)
    rdj_queue_push(QMT_ORDERCB, json.dumps({"topic":"passOrderInfo", "data":data,"msg":msg}, cls=Py36JsonEncoder))
    pub_msg(ct, passOrderInfo, 'error_order')
