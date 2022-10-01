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
from datetime import datetime as dt
import threading
from aare.noqa import *

from QAPUBSUB.consumer import subscriber, subscriber_topic, subscriber_routing

from QAPUBSUB.producer import publisher, publisher_topic

class SUB:
	pass
	
sub_param = SUB()
today_str=datetime.datetime.now().strftime("%Y%m%d")
param=SUB()

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

time_now = dt.now()
trade_start = dt.strptime(time_now.strftime("%Y-%m-%d 09:16:00"), "%Y-%m-%d %H:%M:%S")
trade_open_stop=dt.strptime(time_now.strftime("%Y-%m-%d 09:25:25"), "%Y-%m-%d %H:%M:%S")
trade_mid_stop=dt.strptime(time_now.strftime("%Y-%m-%d 11:30:05"), "%Y-%m-%d %H:%M:%S")
trade_mid_start=dt.strptime(time_now.strftime("%Y-%m-%d 13:00:00"), "%Y-%m-%d %H:%M:%S")
trade_end = dt.strptime(time_now.strftime("%Y-%m-%d 15:01:00"), "%Y-%m-%d %H:%M:%S")

tick_full = publisher_topic(exchange='stock_tick_full',routing_key='', host=eventmq_ip)
tick_amx = publisher_topic(exchange='stock_tick_amx',routing_key='', host=eventmq_ip)
tick_position=publisher_topic(exchange='stock_tick_position',routing_key='', host=eventmq_ip)
tick_open=publisher_topic(exchange='stock_tick_open',routing_key='', host=eventmq_ip)

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
	sub = subscriber_routing(exchange='qmtin_control', routing_key='', host=eventmq_ip)
	sub.callback = qmtin_control
	threading.Thread(target=sub.start, daemon=True).start()
	
	param.code_nost=code_list_to_qmt(jl_read('code_nost'))
	param.code_amx = code_list_to_qmt(jl_read('code_amx'))
	
	#sub_param.full_code = ct.get_stock_list_in_sector('沪深A股')

	if dt.now() < trade_open_stop:
		ct.run_time("open_tick_run","5nSecond","2019-10-14 13:20:00")
		param.open_time_run = True
	else:
		param.open_time_run = False
	
		
	#ct.subscribe_whole_quote(['SH', 'SZ'], full_quote_cb)
	ct.subscribe_whole_quote(param.code_nost, full_quote_cb)
	param.ct=ct


def full_quote_cb(data):
	print(f"whole quote call back: {len(data)}")
	tick_full.pub(json.dumps({'topic': 'quote', 'data': data},cls=MyCustomEncoder ),   routing_key='full')
	#get_min_nday(param.code_nost,'20220930','1m' )
	

def open_tick_run(ct):
	if param.open_time_run and dt.now() > trade_start:
		
		full_tick = ct.get_full_tick(param.code_nost)
		tick_open.pub(json.dumps({'topic': 'open', 'data': full_tick},cls=MyCustomEncoder ),   routing_key='open')
		print(f'open time pub: {len(full_tick)}')
		if dt.now() > trade_open_stop:
			param.open_time_run = False
			
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
	
	
def get_min_nday(codelist,start,type='1m'):
	for code in codelist:
		#df = sub_param.ct.get_market_data(['open','high','low','close','volume'],stock_code=[code],start_time=start,end_time='',dividend_type='front',period=type)
		df = param.ct.get_market_data(['open','high','low','close','volume','amount'],stock_code=[code],start_time=start,end_time='',dividend_type='front',period=type)
		
		print(df)
		
def get_tick_nday(codelist, start_day):
	df=param.ct.get_market_data(['quoter'],stock_code=codelist, start_time=start_day, period='tick')
	print(df)
	return df
	
	
	
def handlebar(ct):
	#get_min_nday(param.code_amx[:3],'20220930',type='1m')
	get_tick_nday(param.code_nost[:1],'20220930')
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
    tick_full.pub(json.dumps({'topic': topics, 'data': h},cls=MyCustomEncoder ),   routing_key='full')


def qmtin_control(ct, a, b, data):
    try:
        print(ct, a, b, data)
        r = json.loads(data)
        print(r)

    except:
        pass

