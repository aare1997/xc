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


@click.command()
@click.option("--sub", default="6000000")
def control(sub):
    qmt_pub = publisher_topic(exchange='qmtin_control', routing_key='', host=eventmq_ip)
    msg = {"contorl": "add_sub"}
    msg['code'] = "600039"
    print(json.dumps(msg))
    qmt_pub.pub(json.dumps(msg), routing_key='')


if __name__ == "__main__":

    control()
