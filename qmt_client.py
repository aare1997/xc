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

DOWNLOAD_MIN_FILE_NAME = "easymoney_download"
TODAY_STR = dt.now().strftime("%Y-%m-%d")
TODAY_DATE = dt.now().date()
TODAY_DATETIME = dt.strptime(f"{TODAY_STR} 00:00:00", "%Y-%m-%d %H:%M:%S")
START_AT0930 = 570
STOP_AT1130 = 690
STOP_AT1300 = 780
STOP_AT1500 = 900

START_AT0924 = 564


def tick_to_min(tick):
    return int(tick[:2]) * 60 + int(tick[3:5])


def klines_to_df(klines, freqs, use_polars=False):
    start = time.time()
    retx = {}
    for x in freqs:
        retx[str(x)] = []
    for code in klines:
        for x in retx:
            data = klines[code][x]["data"]
            if data:
                retx[x].extend(data)
    mid = time.time()
    ret_df = {}
    for x in freqs:
        if use_polars:
            ret_df[str(x) + "min"] = pl.from_dicts(retx[str(x)])
        else:
            ret_df[str(x) + "min"] = pd.DataFrame(retx[str(x)])

    end = time.time()
    # print(f"kline {mid-start:.2f}  dataframe {end-mid:.2f}")

    return ret_df


def new_klines(codex, freqs):
    klines = {}
    for y in codex:
        code_k = {}
        for x in freqs:
            new = {}
            new["freq"] = x
            new["freq_str"] = str(x) + "min"
            new["time_update"] = "00:00:00"
            new["time_start"] = "00:00:00"
            new["time_end"] = "00:00:00"
            new["vol_start"] = 0.0
            new["vol_last"] = 0.0
            new["am_start"] = 0.0
            new["am_last"] = 0.0
            new["data"] = []
            code_k[str(x)] = new
        klines[str(y)] = code_k

    return klines


def tick_update_klines_right(code, tick, klines, date_str, has_time=False):
    kl = klines[code]
    price = float(tick["close"])

    dt_hm = tick_to_min(tick["time"][0:5])
    if dt_hm < START_AT0930:
        dt_hm = 570
        tick["time"] = "09:30:00"

    # print(f'{dt_hm} {tick["time"][0:5]}')
    for x in kl:
        # print(f"befor update {x}\n {kl[x]}")
        divx = int(x)
        old_bar = kl[x]
        dt_end_hm = tick_to_min(old_bar["time_end"][0:5])
        need_new = 0
        if len(old_bar["data"]) > 0:
            if dt_hm >= dt_end_hm:
                need_new = 1
                if dt_hm >= STOP_AT1500 or (dt_hm >= STOP_AT1130 and dt_hm < STOP_AT1300):
                    # print(f"no need {x} new at :{dt_hm}")
                    need_new = 0

        if len(old_bar["data"]) == 0 or need_new == 1:
            new = {}

            new["code"] = code
            new["open"] = price
            new["close"] = price
            new["high"] = price
            new["low"] = price
            new["volume"] = float(tick["volume"]) - kl[x]["vol_last"]
            new["amount"] = float(tick["amount"]) - kl[x]["am_last"]
            # new["type"] = old_bar["freq_str"]
            th = int(tick["time"][0:2])
            tm = int(tick["time"][3:5])
            if int(x) == 60 and (th == 10 or th == 9):
                # new_time= "{}:{}:00".format(str(th).zfill(2), str(int(tm / divx) * divx).zfill(2))
                if th == 9:
                    new_time = "09:30:00"
                else:
                    new_time = "10:30:00"

            else:
                new_time = "{}:{}:00".format(str(th).zfill(2), str(int(tm / divx) * divx).zfill(2))

            kl[x]["time_start"] = new_time
            end_time = dt.strptime(new_time, "%H:%M:%S") + timedelta(minutes=divx)
            end_time_str = end_time.strftime("%H:%M:00")
            kl[x]["time_end"] = end_time_str
            kl[x]["time_update"] = tick["time"]
            new["datetime"] = dt.strptime(f"{TODAY_STR} {end_time_str}", "%Y-%m-%d %H:%M:%S")
            kl[x]["data"].append(new)

            if len(kl[x]["data"]) > 1:
                kl[x]["vol_start"] = kl[x]["vol_last"]
                kl[x]["am_start"] = kl[x]["am_last"]
            # print(f"new {x} :\n {kl[x]}")

        else:
            last = old_bar["data"][-1]
            if price > last["high"]:
                last["high"] = price
            if price < last["low"]:
                last["low"] = price
            last["close"] = price
            last["amount"] = float(tick["amount"]) - kl[x]["am_start"]
            last["volume"] = float(tick["volume"]) - kl[x]["vol_start"]

            kl[x]["time_update"] = tick["time"]
            kl[x]["am_last"] = float(tick["amount"])
            kl[x]["vol_last"] = float(tick["volume"])
            # if int(x) > 1:
            #     print(f"update {x}:\n {kl[x]}")


def qmt_tick_update(data, codelist, freqs, klines):

    day_list = []
    jrzt = []

    for k in data:
        v = data[k]

        new = {}
        new["code"] = k[:6]
        if new["code"] not in codelist:
            continue
        new["close"] = float(v["lastPrice"])
        new["high"] = float(v["high"])
        new["low"] = float(v["low"])
        new["open"] = float(v["open"])
        new["volume"] = float(v["volume"])
        new["amount"] = float(v["amount"])
        if new["close"] == "-" or new["amount"] == 0.0:
            continue

        new["date"] = TODAY_DATETIME
        new["time"] = dt.now().strftime("%H:%M:00")
        tick_update_klines_right(new["code"], new, klines, TODAY_STR)
        del new["time"]

        day_list.append(new)
        if (v["lastPrice"] / v["lastClose"] - 1) * 100 > 7.0:

            if ((v["high"] - v["lastClose"]) / (v["lastClose"])) > 0.09:
                jrzt.append(new["code"])

    dfs = klines_to_df(klines, freqs)

    day = pd.DataFrame(day_list)

    dfs["day"] = day
    update_jrzt(jrzt)
    return dfs


g_jrzt = []
g_jrzt_change = []
g_jrzt_last_time = dt.now()
g_jrzt_last_need_update = False


def update_jrzt(jrzt):
    global g_jrzt
    global g_jrzt_change
    global g_jrzt_last_time
    global g_jrzt_last_need_update

    if len(g_jrzt) > 0:
        if len(list(set(g_jrzt) - set(jrzt))) == 0:
            return
    jrzt_change = len(jrzt) - len(g_jrzt)
    if abs(jrzt_change) > 5:
        print(f"Tick : {dt.now().strftime('%H:%M:%S')},  jrzt change fast! {jrzt_change}")
        if jrzt_change > 5 and jrzt_change < 30:

            new_change = list(set(jrzt) - set(g_jrzt))
            g_jrzt_change.append(new_change)
            g_jrzt_last_time = dt.now()
            g_jrzt_last_need_update = True
            jltmp_write(new_change, key="tick_jrzt_change")

    if g_jrzt_last_need_update == True and dt.now() > (g_jrzt_last_time + timedelta(minutes=10)):
        g_jrzt_last_need_update = False
        jltmp_write([], key="tick_jrzt_change")

        print(f"Tick : {dt.now().strftime('%H:%M:%S')},  jrzt change clear!")

    g_jrzt = jrzt
    jltmp_write(g_jrzt, key="tick_jrzt")


def tick_open_update_klines(code, tick, klines):
    kl = klines[code]
    price = float(tick["bidPrice"])

    dt_hm = tick_to_min(tick["time"][0:5])
    if dt_hm < START_AT0930:
        dt_hm = 570
        tick["time"] = "09:30:00"

    for x in kl:
        divx = int(x)
        old_bar = kl[x]
        dt_end_hm = tick_to_min(old_bar["time_end"][0:5])
        need_new = 0
        if len(old_bar["data"]) > 0:
            if dt_hm >= dt_end_hm:
                need_new = 1
                if dt_hm >= STOP_AT1500 or (dt_hm >= STOP_AT1130 and dt_hm < STOP_AT1300):
                    # print(f"no need {x} new at :{dt_hm}")
                    need_new = 0

        if len(old_bar["data"]) == 0 or need_new == 1:
            new = {}

            new["code"] = code
            new["open"] = price
            new["close"] = price
            new["high"] = price
            new["low"] = price
            new["volume"] = float(tick["volume"]) - kl[x]["vol_last"]
            new["amount"] = float(tick["amount"]) - kl[x]["am_last"]
            new['bidPrice'] = tick['bidPrice']
            new['bidVol'] = tick['bidVol']
            new['bidAmount'] = tick['bidAmount']
            new['askPrice'] = tick['askPrice']
            new['askVol'] = tick['askVol']
            new['askAmount'] = tick['askAmount']

            th = int(tick["time"][0:2])
            tm = int(tick["time"][3:5])
            if int(x) == 60 and (th == 10 or th == 9):
                # new_time= "{}:{}:00".format(str(th).zfill(2), str(int(tm / divx) * divx).zfill(2))
                if th == 9:
                    new_time = "09:30:00"
                else:
                    new_time = "10:30:00"

            else:
                new_time = "{}:{}:00".format(str(th).zfill(2), str(int(tm / divx) * divx).zfill(2))

            kl[x]["time_start"] = new_time
            end_time = dt.strptime(new_time, "%H:%M:%S") + timedelta(minutes=divx)
            end_time_str = end_time.strftime("%H:%M:00")
            kl[x]["time_end"] = end_time_str
            kl[x]["time_update"] = tick["time"]
            new["datetime"] = dt.strptime(f"{TODAY_STR} {end_time_str}", "%Y-%m-%d %H:%M:%S")
            kl[x]["data"].append(new)

            if len(kl[x]["data"]) > 1:
                kl[x]["vol_start"] = kl[x]["vol_last"]
                kl[x]["am_start"] = kl[x]["am_last"]
            # print(f"new {x} :\n {kl[x]}")

        else:
            last = old_bar["data"][-1]
            if price > last["high"]:
                last["high"] = price
            if price < last["low"]:
                last["low"] = price
            last["close"] = price
            last["amount"] = float(tick["amount"]) - kl[x]["am_start"]
            last["volume"] = float(tick["volume"]) - kl[x]["vol_start"]
            last['bidPrice'] = tick['bidPrice']
            last['bidVol'] = tick['bidVol']
            last['bidAmount'] = tick['bidAmount']
            last['askPrice'] = tick['askPrice']
            last['askVol'] = tick['askVol']
            last['askAmount'] = tick['askAmount']

            kl[x]["time_update"] = tick["time"]
            kl[x]["am_last"] = float(tick["amount"])
            kl[x]["vol_last"] = float(tick["volume"])
            # if int(x) > 1:
            #     print(f"update {x}:\n {kl[x]}")


def qmt_open_tick(data, codelist, freqs, klines):
    day_list = []
    for k in data:
        v = data[k]

        new = {}
        new["code"] = k[:6]
        if new["code"] not in codelist:
            continue
        new["close"] = float(v["lastPrice"])
        new["high"] = float(v["high"])
        new["low"] = float(v["low"])
        new["open"] = float(v["open"])
        new['askPrice'] = float(v['askPrice'][0])
        new['askVol'] = float(v['askVol'][0])
        new['askAmount'] = new['askPrice'] * new['askVol'] * 100
        new['bidPrice'] = float(v['bidPrice'][0])
        new['bidVol'] = float(v['bidVol'][0])
        new['bidAmount'] = new['bidPrice'] * new['bidVol'] * 100
        new["volume"] = float(v["volume"])
        new["amount"] = float(v["amount"])
        if new['bidAmount'] == 0.0 or new['askAmount'] == 0.0:
            continue

        new["date"] = TODAY_DATETIME
        new["time"] = dt.now().strftime("%H:%M:%S")
        tick_open_update_klines(new["code"], new, klines)
        del new["time"]

        day_list.append(new)

    dfs = klines_to_df(klines, freqs)
    day = pd.DataFrame(day_list)
    dfs["day"] = day

    return dfs


def run_open():
    exchange = "stock_tick_open"
    routing_key = "open"
    print("Running open mode")
    x = subscriber_topic(host=eventmq_ip, exchange=exchange, routing_key=routing_key)
    freqs = [1]
    kline = new_klines(stock_code, freqs)

    def callback(a, b, c, data):
        jdata = orjson.loads(data)["data"]
        # print(jdata)

        dfs = qmt_open_tick(jdata, stock_code, freqs, kline)

        for x in dfs:
            print(x, dfs[x])

    x.callback = callback
    x.start()


stock_code = jl_read("code_nost")
stock_freqs = [1, 15, 30, 60]


def run_quote():
    exchange = "stock_tick_full"
    routing_key = "full"
    print("Running quote full mode")
    x = subscriber_topic(host=eventmq_ip, exchange=exchange, routing_key=routing_key)

    kline = new_klines(stock_code, stock_freqs)

    def callback(a, b, c, data):
        jdata = orjson.loads(data)["data"]
        print(jdata)
        dfs = qmt_tick_update(jdata, stock_code, stock_freqs, kline)
        for x in dfs:
            print(x, dfs[x])

    x.callback = callback
    x.start()


@click.command()
@click.option("--code", default="rb1910")
@click.option("--open", is_flag=True, default=False)
def sub(code, open):
    if open:

        run_open()
    else:
        run_quote()


if __name__ == "__main__":

    sub()
