# -*- coding: utf-8 -*-
import os
import sys
curPath = os.path.abspath(os.path.dirname(__file__))
parentPath = os.path.abspath(os.path.dirname(curPath))
sys.path.append(curPath)
sys.path.append(parentPath)
import requests
import pandas as pd
import json
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

data = {"username": "tfqhyjs", "password": "tfqhyjs", "code": "8adb0547dd2976ae8105ed459b296568",
        "ip": "27.115.66.146"}
req = json.loads(requests.post('https://open.sci99.com/h3/v2/system/user/login', json=data).text)
headers = {
    'auth': req['data'],
    'ciikie': 'route=258ceb4bb660681c2cb2768af9756936'
}
req = requests.get('https://open.sci99.com/h3/v1/mycustom/dataitemmerge/108739/week', headers=headers)
info_data = list()
for info in req.json()['data']['datalist'][:3]:
    info_data.append(['乙二醇MTO税前装置毛利(周)','eg',info['datetime'],info['108739'],'ht3_mtoml_week'])
if len(info_data) > 0:
    add_data('huagong_data', info_data)