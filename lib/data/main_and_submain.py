"""
生成主力和次主力合约
"""

import pymongo
import pandas as pd
import re
from datetime import datetime, timedelta

pd.set_option('display.max_columns', 12)
pd.set_option('display.width', 200)
cmd_list = ['L.DCE', 'PP.DCE', 'I.DCE', 'J.DCE', 'JM.DCE', 'M.DCE', 'C.DCE', 'RB.SHF', 'BU.SHF', 'RU.SHF', 'NI.SHF',
            'HC.SHF', 'TA.CZC', 'MA.CZC', 'AP.CZC', 'ZC.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE', 'EG.DCE', 'SP.SHF',
            'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE']

conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
future_collection = db['FuturesMD']
deriv_collection = db['DerivDB']

def seize_code(full_code):
    ptn1 = re.compile('[A-za-z]+(?=\d+\.)')
    res1 = ptn1.search(full_code).group()
    ptn2 = re.compile('(?<=([A-Za-z]))\d+(?=\.)')
    res2 = ptn2.search(full_code).group()
    ptn3 = re.compile('(?<=\d\.)[A-Za-z]+')
    res3 = ptn3.search(full_code).group()
    return res1 + res2[-3:] + '.' + res3

for cmd in cmd_list:
    print(cmd)
    ptn1 = re.compile('\w+(?=\.)')
    res1 = ptn1.search(cmd).group()
    ptn2 = re.compile('(?<=\.)\w+')
    res2 = ptn2.search(cmd).group()
    queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}}
    projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM']
    record = future_collection.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(record)
    df.drop(columns='_id', inplace=True)
    df['fake_code'] = df['wind_code'].apply(func=seize_code)
    wind_code_list = set(df['wind_code'].values.flatten())
    for w in wind_code_list:
        df_w = df[df['wind_code'] == w]
        df_w.sort_values(by='date', ascending=True, inplace=True)
        df_w['OI_10_MA'] = df_w[['OI']].rolling(window=10).mean()
        print(df_w)
    df.drop_duplicates(
        subset=['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM', 'fake_code'], inplace=True)

    df.drop(columns='fake_code', inplace=True)
    df_group = df.groupby(by='date')
    # print(df_group.apply(func=lambda x: pd.Series(x[x['OI'] == x['OI'].max()].values.flatten(), index=x.columns)))
