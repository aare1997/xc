import numpy as np
import pandas as pd
import datetime
import toml
import time
import jsonlines as jsonl
from pathlib import Path
import feather

import random
import string
import shutil
import json


def toml_to_file(ctx, file):
    with open(file, "w") as f:
        new_string = toml.dump(ctx, f)


def toml_load_file(file):
    return toml.load(file)


STOCK_TMP_PATH = "Z:\\TEMP\\stock\\"
Path(STOCK_TMP_PATH).mkdir(exist_ok=True)


def ft_read_df(name="", key_str="", use_big_lock=False, reset=False):
    file = STOCK_TMP_PATH + name + "_" + key_str + ".feather"

    data = feather.read_dataframe(file)
    if reset:
        if "date" in data.columns:
            data = data.assign(date=pd.to_datetime(data.date))
            data = data.set_index(["date", "code"])
        else:
            data = data.assign(datetime=pd.to_datetime(data.datetime))
            data = data.set_index(["datetime", "code"])

    return data


def ft_write_df(df, name="", key_str=""):
    random_str = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
    random_name = STOCK_TMP_PATH + random_str

    old_name = STOCK_TMP_PATH + name + "_" + key_str + ".feather"
    feather.write_dataframe(df, random_name)
    try:
        shutil.move(random_name, old_name)
    except Exception as e:
        pass


def ft_lock(name="", big_lock=True):
    file_big_lock = Path(STOCK_TMP_PATH + name + ".lock")
    if big_lock:
        file_big_lock.touch()
    else:
        file_big_lock.unlink()


def file_lock_try(name="", lock=True, sleep=True):
    file_lock = Path(STOCK_TMP_PATH + name + ".lock")
    sleep_times = 12
    if file_lock.exists() and lock == False:
        # print(f"ulock")
        file_lock.unlink()
        return

    while file_lock.exists():
        time.sleep(10)
        sleep_times -= 1
        # print(f"lock sleep {name}")
        print(f"lock . ", end="")
        if sleep_times == 0:
            return
    if file_lock.exists() == False and lock == True:
        # print(f"lock")
        file_lock.touch()
        return


def file_lock_wait(name="", sleep_count=12):
    file_lock = Path(STOCK_TMP_PATH + name + ".lock")
    sleep_times = sleep_count
    while file_lock.exists():
        time.sleep(10)
        sleep_times -= 1
        # print(f"lock sleep {name}")
        if sleep_times == 0:
            return


def ft_realtime_ok(name="sina_tick", key_str="day"):
    file = Path(STOCK_TMP_PATH + name + "_" + key_str + ".feather")
    return file.exists()


def ft_mongo_ram_ok(name="mongo_all", key_str="day"):
    file = Path(STOCK_TMP_PATH + name + "_" + key_str + ".feather")
    return file.exists()


def code_from_tdx(name):
    tdx_file = "C:\\zd_zts\\T0002\\blocknew\\{}.blk".format(name)
    code_list = []
    with open(tdx_file, "r") as f:
        for line in f:
            real_code = line[1:].rstrip()

            if (
                len(real_code) == 0
                or len(real_code) > 6
                or real_code[0] == "8"
                or real_code[:3] == "399"
                or real_code > "900000"
            ):
                continue

            code_list.append(real_code)
    return code_list


def save_index_to_tdx(code_list, name, mode="w"):
    tdx_file = "C:\\zd_zts\\T0002\\blocknew\\{}.blk".format(name)
    stock_list = code_list
    with open(tdx_file, mode) as f:
        for code in stock_list:
            if code > "600000" or code < "100000":
                f.write("1")
            else:
                f.write("0")

            f.write(code)
            f.write("\n")


TEMP_JSONL_FILE = "z:/temp/stock_tmp.jsonl"
SAVED_JSONL_FILE = "d:/Trader/aq/saved_json.jsonl"


def jl_write(data, key_str, file_name=None, position="hd", mode="w"):

    random_str = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))

    if file_name is not None:
        fname = file_name
    elif position in ["tmp", "ram"]:
        fname = TEMP_JSONL_FILE
    else:
        fname = SAVED_JSONL_FILE

    random_name = fname + random_str

    if mode in ["n"]:
        open_mode = "w"
    else:
        open_mode = "a"

    content = {}

    if mode in ["w"]:
        if Path(fname).exists():
            with jsonl.open(fname, mode="r") as reader:
                for row in reader:
                    for k, v in row.items():
                        content[k] = v
        content[key_str] = data

        with jsonl.open(random_name, mode="w") as writer:
            for k, v in content.items():
                writer.write({k: v})
        shutil.move(random_name, fname)

    if mode in ["n"]:
        with jsonl.open(fname, mode=open_mode) as writer:
            writer.write({key_str: data})


def jl_read(key_str, default=[], position="hd", file_name=None):
    if file_name is not None:
        fname = file_name
    elif position in ["tmp", "ram"]:
        fname = TEMP_JSONL_FILE
    else:
        fname = SAVED_JSONL_FILE
    try:
        with jsonl.open(fname, mode="r") as reader:
            for row in reader:
                # pprint.pprint(row)
                for k, v in row.items():

                    if k == key_str:
                        return v
    except Exception as e:
        return default
    print(f"{key_str} is {default}")
    return default


def jltmp_write(data, key):
    jl_write(data, key, file_name=TEMP_JSONL_FILE)


def jltmp_read(key, default=[], file_name=None):
    return jl_read(key, default=default, position="tmp", file_name=file_name)


TEMP_JSONL_DF_FILE = "z:/temp/stock_tmp_df.jsonl"
SAVED_JSONL_DF_FILE = "d:/Trader/aq/saved_df_json.jsonl"


def jldf_write(data, key_str, file_name=None, position="hd", mode="w"):

    if file_name is not None:
        fname = file_name
    elif position in ["tmp", "ram"]:
        fname = TEMP_JSONL_DF_FILE
    else:
        fname = SAVED_JSONL_DF_FILE

    if mode in ["n"]:
        open_mode = "w"
    else:
        open_mode = "a"

    content = {}
    # current_date = datetime.datetime.now().strftime("%Y-%d-%d %H:%M:00")
    current_date = datetime.datetime.now().strftime("%H:%M:%S")
    data["datetime"] = current_date

    if mode in ["w"]:
        if Path(fname).exists():
            with jsonl.open(fname, mode="r") as reader:
                for row in reader:
                    for k, v in row.items():
                        content[k] = v

        if key_str in content:
            content[key_str].append(data)
        else:
            content[key_str] = [data]

        with jsonl.open(fname, mode="w") as writer:
            for k, v in content.items():
                writer.write({k: v})

    if mode in ["n"]:
        with jsonl.open(fname, mode=open_mode) as writer:
            writer.write({key_str: data})


def jldf_read(key, default={}, file_name=None, position="hd"):
    if file_name is not None:
        fname = file_name
    else:
        if position in ["tmp", "ram"]:
            fname = TEMP_JSONL_DF_FILE
        else:
            fname = SAVED_JSONL_DF_FILE
    return jl_read(key, default=default, file_name=fname)


def jldf_tmp_write(data, key_str, position="tmp"):
    jldf_write(data, key_str, file_name=None, position="tmp", mode="w")


def jldf_tmp_read(key, position="tmp"):
    return jldf_read(key, position=position)


def jldf_tmp_read_display(key, position="tmp", limit=10):
    pd.set_option("display.max_rows", None)
    orign = jldf_tmp_read(key, position=position)

    if orign:
        df = pd.DataFrame(orign).set_index("datetime")
        df_len = len(df)
        if df_len > limit:
            steps = 5 - (df_len // 10)
            drop_list = [i for i in range(0, df_len - 5, steps)]
            # print(steps, drop_list)
            df = pd.DataFrame(orign).drop(drop_list).set_index("datetime")
        print(f"\n{df}")
    return orign


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return str(obj)
        else:
            return super(NpEncoder, self).default(obj)


def get_min_from_open():
    t_open = datetime.datetime.strptime(datetime.date.today().strftime("%Y-%m-%d 9:30:00"), "%Y-%m-%d %H:%M:%S")
    t_now = datetime.datetime.now()
    th = 1
    if t_now > t_open:
        th = max((t_now - t_open).seconds // 60, 1)

    real_min = th

    if th > 120 and th < 210:
        real_min = 120
    if th > 210 and th < 330:
        real_min = th - 210 + 120
    if th > 330:
        real_min = 240

    return real_min


def code_append_market(code):
    if code[0] == "6":
        return code + '.SH'
    if code[0] == "3":
        return code + ".SZ"
    if code[0] == '0':
        return code + ".SZ"
    return code


def code_list_to_qmt(codex):
    return [code_append_market(x) for x in codex]


QMT_EVENT_PATH = "Z:\\TEMP\\qmt\\"


def has_qmt_evnt(key='sub_tick'):
    file_name = QMT_EVENT_PATH + key
    event_path = Path(QMT_EVENT_PATH)
    event_list = list(event_path.glob(key + "*"))
    if event_list:
        ret = None
        with open(event_list[0]) as f:
            ret = json.load(f)
        event_list[0].unlink()
        return ret
    return None


def create_qmt_file_event(jsontext, key='sub_tick'):
    event_name = Path(QMT_EVENT_PATH + key)
    if event_name.exists():
        random_str = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
        event_name = Path(QMT_EVENT_PATH + key + random_str)
    with open(event_name, 'w') as f:
        f.write(jsontext)


class Py36JsonEncoder(json.JSONEncoder):
    def iterencode(self, obj, _one_shot=False):

        if isinstance(obj, float):
            yield format(obj, '.2f')
        elif isinstance(obj, dict):
            last_index = len(obj) - 1
            yield '{'
            i = 0
            for key, value in obj.items():
                yield '"' + key + '": '
                for chunk in Py36JsonEncoder.iterencode(self, value):
                    yield chunk
                if i != last_index:
                    yield ","
                i += 1
            yield '}'
        elif isinstance(obj, list):
            last_index = len(obj) - 1
            yield "["
            for i, o in enumerate(obj):
                for chunk in Py36JsonEncoder.iterencode(self, o):
                    yield chunk
                if i != last_index:
                    yield ","
            yield "]"
        else:
            for chunk in json.JSONEncoder.iterencode(self, obj):
                yield chunk
