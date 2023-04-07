# -*- coding: utf-8 -*-
import sys
import os
import json
import time
curPath = os.path.abspath(os.path.dirname(__file__))
parentPath = os.path.abspath(os.path.dirname(curPath))
sys.path.append(curPath)
sys.path.append(parentPath)
import asyncio
import datetime
import requests
import pandas as pd
import multiprocessing
from sqlalchemy import create_engine
from database.sqlserver_base import update_to_sql2
from utils.send_mail import send_mail


def get_token():
    headers = {
        'appkey': 'realtime',
        'authorization': 'Bearer eyJraWQiOiJkZWZhdWx0IiwieDV0IjoiYjdGODNuX3hxYUtCZmVhNW9oVERIVnhlbEtvIiwiYWxnIjoiUlMyNTYifQ.eyJleHAiOjE2ODA4OTQxMDAsImp0aSI6InNyNjIzQ2FWSXFEVXU4cndDOUJ4Z1EiLCJpYXQiOjE2ODA4MjkzMDAsInN1YiI6IkNIRU5IQU8wMjYxODhAR1RKQVMuQ09NIiwic2Vzc2lvbl9pZCI6ImRjTkhWYk01VUozTHNCeERla3RNSHc9PX5rclVhY1FyYnpjWW1xSEEzaTFZM2UwbVRKQ2lMc1lOdTk2d1pic0hjakYxTzM5N1lYejdZNkd2RE9xWFN2RDBtU2l6bWZXaEp2ckltN1BYN2l3SjJYWWRXQTZkTlRaUjFlajlpMENTNDFCd1d5bzcyTzA5TDMwdTNZc0ZMdmk3USIsIm1kY19zc29fbGluayI6ImkzaUhHTTZURmZKNWNsRWpZOS9yZnc9PX5iNnhSZjFBMzBlTE9jbVlaaHpTUEk0M2ZSSW1tYXhGRENnTzd2T25Gc1ZoT3VWckVOeU1jSDhsemsyNkJwcTFhIiwiZG9tYWluIjoiZGVmYXVsdCJ9.mR3RLOzns2P-2-4370PyLC7R53pYvGoi8M07MrTW9xHHZT9YOSRV7KGLP1WrbkAhaJfrh88x_inO-YF4moZwL_A4iYhZHHZjLxjdAj9pNIWAHdO6_zUpk22q3nyjaMbcdca1CtwVHOeYHfm-1bZzPCrKprgaaCN8DrgQhunVBhP_eUCjsUe0RwXL2kCy8yHovJ6JXDSLVEWEfwWGOvnzChRHMxu_dAtpx4aAKZ3D7wNwQ6rRIQzMuH9wL0guzkAxoBZ4g_IrvarDACQBIGq-FH0H8emMtqzyRVDkWeI4TtO9xhuz59xjj_65pCHbas582nHbAe1A9hQwkwkshocJPQ'
    }

    req = requests.get('https://api.realtime.platts.com/platts-platform/heards/v1/token/oauth', headers=headers)
    print('get_token', req.text)
    return req.json()['access_token']


def add_data(data_table_name, data):
    # 添加信息到数据库
    table_name = data_table_name
    df_result = pd.DataFrame(data)
    df_result.columns = ['key1', 'symbol', 'trade_date', 'f1', 'data_key']
    df_result = pd.concat([df_result, pd.DataFrame(columns=['key2', 'key3', 'key4', 'key5', 'key6'])])
    df_result['key2'] = '-'
    df_result['key3'] = '-'
    df_result['key4'] = '-'
    df_result['key5'] = '-'
    df_result['key6'] = '-'
    update_result = update_to_sql2(table_name, df_result,
                                   ['trade_date', 'symbol', 'data_key', 'key1', 'key2', 'key3', 'key4', 'key5',
                                    'key6'],
                                   if_exists='append', index=False)
    return update_result


def index_exists(name, key):
    # 此函数，判断指标是否存在，并返回最大日期
    engine = create_engine("mssql+pymssql://sa:Th@nfS@@192.168.78.160:1433/TFFY_PETRO?charset=utf8")
    data = pd.read_sql(
        f"select * from [TFFY_PETRO].[dbo].[行情_商品价格指标列表] where 指标名称 = '{name}' and 指标来源='TFFY_import'",
        engine)
    if len(data) == 0:
        sql = f"SELECT [trade_date],[f1] FROM [TFFY_import].[dbo].[huagong_data] where [key1]=''{name}'' and [data_key]=''Platts_{key}_day'' and [trade_date] > (select case when max(报价日期) is null then ''2000-01-01'' else max(报价日期) end from tffy_petro.dbo.行情_商品报价表 where 品名=''{name}'')"
        try:
            global create_index
            create_index += 1
            pd.read_sql(
                f"INSERT INTO [TFFY_PETRO].[dbo].[行情_商品价格指标列表]([指标分类],[指标ID],[指标名称],[指标来源],[指标说明],[指标语句],[增加方式],[更新时间],[刷新语句],[Wind指标项],[更新次序]) VALUES ('原油','{name}', '{name}', 'TFFY_IMPORT', null, '{sql}', '增量', null, null, null, 1)",
                engine)
        except:
            return '2000-01-01'
    else:
        if pd.read_sql(f"select max(报价日期) from tffy_petro.dbo.行情_商品报价表 where 品名='{name}'", engine).iloc[0][
            0] == None:
            return '2000-01-01'
        else:
            return str(pd.read_sql(f"select max(报价日期) from tffy_petro.dbo.行情_商品报价表 where 品名='{name}'",
                                   engine).iloc[0][0])[:10]


def run(in_q, out_q, header):
    url = 'https://api.realtime.platts.com/platts-platform/search/v1/chart/highcharts?Symbol={}&ToDate=' + datetime.datetime.now().strftime(
        "%Y%m%d")
    while in_q.empty() is not True:
        meg = in_q.get()
        print(in_q.qsize())
        req = requests.get(url=url.format(meg[0]), headers=header)
        for info in json.loads(req.text)[-3:]:
            dateArray = time.localtime(int(info[0] / 1000))
            out_q.put([meg[1], time.strftime("%Y-%m-%d", dateArray), info[1], meg[0]])
        in_q.task_done()
        return out_q


def split_list(x, n):
    newList = []
    if len(x) <= n:
        newList.append(x)
        return newList
    else:
        newList.append(x[:n])
        return split_list(x[n:], n)


def updata_key(list_key):
    dic = {}
    dat = {}
    dic["spider_file"] = os.path.basename(__file__)
    dic["token"] = "e6c2c509d084e3de1b1af39f527e24fce9816865d90c31fcc4b2395e"
    url = 'https://i.thanf.com/apis/spiderIndexCalculate.cfm'
    for key in split_list(list(list_key), 300):
        dat['tffy_petro.dbo.行情_商品价格指标列表'] = key
        dic["data"] = dat
        rep = requests.post(url=url, data=json.dumps(dic, ensure_ascii=False).encode("utf-8"))
        return rep.text


if __name__ == '__main__':
    # 定义变量，记录程序开始时间
    s_time = datetime.datetime.now()
    header = {
        'authority': 'api.realtime.platts.com',
        'method': 'OPTIONS',
        'path': '/platts-platform/search/v1/chart/highcharts?Symbol=ANWOY00&ToDate=20220421',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'access-control-request-headers': 'access-control-allow-origin,appkey,authorization,authtype,x-correlation-id',
        'access-control-request-method': 'GET',
        'origin': 'https://platform.platts.spglobal.com',
        'referer': 'https://platform.platts.spglobal.com/',
        'appkey': 'realtime',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
        'authorization': f'Bearer {get_token()}'}

    # 定义进程池子
    queue = multiprocessing.Manager().Queue()
    result_queue = multiprocessing.Manager().Queue()
    from_data = pd.read_excel(r'D:\project\tffy_spider\data_spider\普氏指标.xlsx', header=None, sheet_name=None)
    # 定义一个列表存放url数据
    symbol_list = []
    for i in from_data:
        new_data = from_data[i]
        # 过滤掉空ID指标
        new_data.dropna(axis=0, how='any', inplace=True)
        # # # 定义列表，存放待执行指标
        [symbol_list.append([i, j]) for i, j in zip(list(new_data[1]), list(new_data[0]))]
    for u in symbol_list:
        queue.put([u[0], u[1]])
    pool = multiprocessing.Pool(20)
    for index in range(queue.qsize()):
        pool.apply_async(run, args=(queue, result_queue, header))
    pool.close()
    pool.join()
    queue.join()
    list_key = set()
    info_data = []
    req = 0
    for i in range(result_queue.qsize()):
        data = result_queue.get()
        list_key.add(data[0])
        info_data.append([data[0], 'petro', data[1], data[2], f'platts_{data[3]}_day'])
    if len(info_data)>0:
        add_data('huagong_date', info_data)
        req = updata_key(list_key)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    content = f'程序名称：{os.path.basename(__file__)}\n已更新：{len(list_key)}条\n更新状态：{req}\n共用时：{datetime.datetime.now()-s_time}'
    loop.run_until_complete(send_mail(mail_list=['haoluhang@thanf.com'], content=content, subject="程序异常报警邮件"))
