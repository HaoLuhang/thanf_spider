# -*- coding: utf-8 -*-
import os
import time
import json
import requests
import pandas as pd
from database.sqlserver_base import update_to_sql2

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

def update_key():
    dic = {}
    dat = {}
    dic["spider_file"] = os.path.basename(__file__)
    dat["tffy_macro.dbo.行情_商品价格指标列表"] = ['ISES太阳周期太阳黑子数','ISES太阳周期F10.7厘米射电辐射通量']
    dic["data"] = dat
    dic["token"] = "e6c2c509d084e3de1b1af39f527e24fce9816865d90c31fcc4b2395e"

    url='http://192.168.78.104/apis/spiderIndexCalculate.cfm'
    rep = requests.post(url=url, data=json.dumps(dic, ensure_ascii=False).encode("utf-8"))
    print(rep)

req = requests.get('https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json')

info_data = list()
for info in req.json()[-3:]:
    info_data.append(['ISES太阳周期太阳黑子数','ssn',info['time-tag']+'-01',info['ssn'],'swpc_ssn_month'])
    info_data.append(['ISES太阳周期F10.7厘米射电辐射通量','ssn',info['time-tag']+'-01',info['f10.7'],'swpc_ssn_month'])

if len(info_data) > 0:
    add_data('huagong_data', info_data)
    update_key()