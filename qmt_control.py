from QUANTAXIS.QAPubSub.consumer import subscriber, subscriber_topic, subscriber_routing
from QUANTAXIS.QAPubSub.producer import publisher, publisher_topic, publisher_routing

import click
import time
import polars as pl
import pandas as pd
import orjson
from datetime import datetime as dt, timezone, timedelta, date
from noqa import *


mongo_ip = "127.0.0.1"
eventmq_ip = "127.0.0.1"
account_cookie = "230041917"
stock_code = jl_read('code_amx')


def start_sub_tick():
    exchange = "stock_tick_amx"
    routing_key = "tick"
    print("sub tick start")
    from QUANTAXIS.QAPubSub.consumer import subscriber, subscriber_topic, subscriber_routing

    x = subscriber_topic(host=eventmq_ip, exchange=exchange, routing_key=routing_key)
    freqs = [1]

    def callback(a, b, c, data):
        jdata = orjson.loads(data)["data"]
        print('------in sub min----------')
        print(jdata)

    x.callback = callback
    x.start()


def start_sub_min():
    exchange = "stock_tick_amx"
    routing_key = "min"
    print("sub min start")
    from QUANTAXIS.QAPubSub.consumer import subscriber, subscriber_topic, subscriber_routing

    x = subscriber_topic(host=eventmq_ip, exchange=exchange, routing_key=routing_key)

    def callback(a, b, c, data):
        jdata = orjson.loads(data)["data"]
        print('------in sub min----------')
        print(jdata)

    x.callback = callback
    x.start()


def test_tick():
    code = ['300146.SZ']
    msg = {"control": "get_tick", "code": code}

    print(json.dumps(msg))
    create_qmt_file_event(json.dumps(msg))
    start_sub_tick()

    # qmt_pub.pub(json.dumps(msg), routing_key='')


def test_min():
    code = ['300146.SZ', '600003.SH']
    msg = {"control": "get_min", "code": code, "period": "1m"}

    print(json.dumps(msg))
    create_qmt_file_event(json.dumps(msg))
    start_sub_min()
    # qmt_pub.pub(json.dumps(msg), routing_key='')


@click.command()
@click.option("--sub", default="6000000")
def control(sub):
    # qmt_pub = publisher_routing(exchange='qmtin_control', routing_key='', host=eventmq_ip)
    # start_sub_tick()
    # start_sub_min()
    msg = {"control": "add_sub"}
    msg['code'] = "600039"
    print(json.dumps(msg))
    # qmt_pub.pub(json.dumps(msg), routing_key='')
    # test_tick(qmt_pub)
    create_qmt_file_event(json.dumps(msg))
    # test_tick()
    test_min()


if __name__ == "__main__":

    control()
