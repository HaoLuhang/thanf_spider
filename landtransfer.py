# -*- coding: utf-8 -*-
import datetime

import requests
import pandas as pd
import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
parentPath = os.path.abspath(os.path.dirname(curPath))
sys.path.append(curPath)
sys.path.append(parentPath)
from sqlalchemy import create_engine
from database.sqlserver_base import update_to_sql2, read_sql

connstr = "mssql+pymssql://fy1h:fy1h@192.168.78.160:1433/TFFY_import?charset=utf8"
engine = create_engine(connstr, echo=True, max_overflow=5)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
}
sql = "SELECT max([发布时间]) FROM [TFFY_import].[dbo].[LandTransfer]"
max_data = read_sql(sql).loc[0][0]
data_from = {"pageNum": 1, "pageSize": 10000,
             "startDate": str(max_data - datetime.timedelta(hours=0, minutes=0, seconds=-1))}
req = requests.post('https://api.landchina.com/t-ncrzd/list', json=data_from, headers=headers)
if len(req.json()['data']['list']) != 0:
    df = pd.DataFrame(req.json()['data']['list'])
    new_df = pd.DataFrame()
    new_df['行政区'] = df['xzqFullName']
    new_df['标题'] = df['bt']
    new_df['宗地总面积(公顷)'] = df['ncrzdZmj']
    new_df['宗地数量(宗)'] = df['zdSl']
    new_df['最晚拟供应时间'] = df['zwNgysj']
    new_df['发布时间'] = df['fbSj']
    print(new_df)
    '''
    ##### 删除表所有数据engine.execute('delete FROM [TFFY_import].[dbo].[LandTransfer]')
    '''
    new_df.to_sql('LandTransfer', con=engine, if_exists='append', index=False)
