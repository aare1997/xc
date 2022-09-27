#encoding:gbk
'''
本策略事先设定好交易的股票篮子，然后根据指数的CCI指标来判断超买和超卖
当有超买和超卖发生时，交易事先设定好的股票篮子
'''
import pandas as pd
import numpy as np
import json
import queue
import pymongo
import time
import datetime
import threading
import simplejson 

from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher, publisher_topic

class SUB:
	pass
	
sub_param = SUB()
today_str=datetime.datetime.now().strftime("%Y%m%d")


class MyCustomEncoder(json.JSONEncoder):
    def iterencode(self, obj, _one_shot=False):
        
        if isinstance(obj, float):
            yield format(obj, '.2f')
        elif isinstance(obj, dict):
            last_index = len(obj) - 1
            yield '{'
            i = 0
            for key, value in obj.items():
                yield '"' + key + '": '
                for chunk in MyCustomEncoder.iterencode(self, value):
                    yield chunk
                if i != last_index:
                    yield","
                i+=1
            yield '}'
        elif isinstance(obj, list):
            last_index = len(obj) - 1
            yield"["
            for i, o in enumerate(obj):
                for chunk in MyCustomEncoder.iterencode(self, o):
                    yield chunk
                if i != last_index:
                    yield","
            yield"]"
        else:
            for chunk in json.JSONEncoder.iterencode(self, obj):
                yield chunk

code_dict={}
vol_dict={}

mongo_ip = '127.0.0.1'
eventmq_ip = '127.0.0.1'
account_cookie = '230041917'
tick_full = publisher_topic(exchange='stock_tick_full',routing_key='', host=eventmq_ip)

def init(ct):
	#hs300成分股中sh和sz市场各自流通市值最大的前3只股票
    #hs300成分股中sh和sz市场各自流通市值最大的前3只股票
	# ContextInfo.trade_code_list=['601398.SH','601857.SH','601288.SH','000333.SZ','002415.SZ','000002.SZ']
	# ContextInfo.set_universe(ContextInfo.trade_code_list)
	# ContextInfo.accID = '6000000058'
	ct.set_account(account_cookie)
	sub = subscriber_routing(exchange='qmt_sub_control', routing_key='', host=eventmq_ip)
	sub.callback = qmt_sub_control
	threading.Thread(target=sub.start, daemon=True).start()
	
	ct.stock = [ct.stockcode + '.' + ct.market]
	print(ct.stock)
	sub_param.full_code = ct.get_stock_list_in_sector('沪深A股')
	sub_param.vol_dict= {}
	for stock in sub_param.full_code:
		sub_param.vol_dict[stock] = ct.get_last_volume(stock)
	ct.run_time("f","3nSecond","2019-10-14 13:20:00")#
	sub_param.ct=ct
	#code_dict['hsa']=code_dict['hsa'][:]

	

def f(ct):
	t0 = time.time()
	full_tick = ct.get_full_tick(sub_param.full_code)
	print(len(full_tick))
	get_today_min(sub_param.full_code,today_str)
	#pub_msg(ct,full_tick, 'abc')
	
	#for codex in code_dict['hsa']:


	return
	
	total_market_value = 0
	total_ratio = 0
	count = 0
	for stock in code_dict.hsa:
		ratio = full_tick[stock]['lastPrice'] / full_tick[stock]['lastClose'] - 1
		rise_price = round(full_tick[stock]['lastClose'] *1.2,2) if stock[0] == '3' or stock[:3] == '688' else round(full_tick[stock]['lastClose'] *1.1,2)
		#如果要打印涨停品种
		#if abs(full_tick[stock]['lastPrice'] - rise_price) <0.01:
		#	print(f"涨停品种 {stock} {C.get_stock_name(stock)}")
		market_value = full_tick[stock]['lastPrice'] * code_dict.vol_dict[stock]
		total_ratio += ratio * market_value
		total_market_value += market_value
		count += 1
	#print(count)
	total_ratio /= total_market_value
	total_ratio *= 100
	print(f'A股加权涨幅 {round(total_ratio,2)}% 函数运行耗时{round(time.time()- t0,5)}秒')
	
	
def get_today_min(codelist,start):
	for code in codelist:
		df = sub_param.ct.get_market_data(['open','high','low','close','amount','volume'],stock_code=[code],start_time=start,end_time='',dividend_type='front',period='1m')

		

	pass
	
	
	
def handlebar(ct):
	#get_today_min(sub_param.full_code,today_str)
	return
	print('handlebar now:',datetime.datetime.now())
	full_tick = ct.get_full_tick(code_dict['hsa'])
	date = timetag_to_datetime(ct.get_bar_timetag(ct.barpos), '%Y%m%d %H:%M:%S')
	print('in handlebar:',date,len(full_tick))
	
	return
	
	date = timetag_to_datetime(ct.get_bar_timetag(ct.barpos), '%Y%m%d %H:%M:%S')
	# 获取当根K线的开高低收
	price = ct.get_market_data(['open','high','low','close'], ct.stock, period=ct.period)
	print(ct.stock, ct.barpos)
	print('price: \n',price)
	if isinstance(price,pd.DataFrame):
		print('当前k线时间:',date, '获取当根pric.ehead的开高低收：\n', price.head())
	else:
		print('当前k线时间:',date, '获取当price线的开高低收：\n', price)
		
	# 获取当根k线以及前30跟K线
	price = ct.get_market_data(['open','high','low','close'], ct.stock, period=ct.period, count=30)
	print('price30 :\n', price)
	if isinstance(price,pd.DataFrame):
		print('当前k线时间:',date, '获取当根k线以及前30跟K线：', price.head())
	else:
		print('当前k线时间:',date, '获取当根k线以及前30跟K线：', price)
	
	if ct.is_last_bar():
		# 获取从指定时间开始到当前最新行情（包括盘中动态行情）
		price = ct.get_market_data(['open','high','low','close'], ct.stock, start_time='20220511', period=ct.period)
		if isinstance(price, pd.DataFrame):
			print('price.tail时间开始到当前最新行情（包括盘中动态行情）:\n',price.tail())
		else:
			print('price时间开始到当前最新行情（包括盘中动态行情）:\n',price)
	print('=============================================')

def pub_msg(ct, h, topics):
    print('len:', len(json.dumps({'topic': topics, 'data': h},cls=MyCustomEncoder )))

    tick_full.pub(json.dumps({'topic': topics, 'data': h},cls=MyCustomEncoder ),   routing_key='full')


def qmt_sub_control(ContextInfo, a, b, data):
    try:
        r = json.loads(data)
        print(r)

    except:
        pass

