"""
根据分钟数据重新生成开盘价和收盘价
开盘价以早上9点为准
收盘价以夜盘结束的价格为准
author: YUANJIE
"""

import pymongo
import pandas as pd
from datetime import timedelta, datetime


conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
min_coll = db['FuturesMinMD']
fut_coll = db['FuturesMDCustomized']

cmd_list = ['CU.SHF', 'AL.SHF', 'ZN.SHF', 'NI.SHF', 'PB.SHF', 'SN.SHF', 'AU.SHF', 'AG.SHF']

tb_list = ['sc888']

def newK(x):
    new = pd.Series()
    try:
        new['OPEN'] = x[x['time'] == '09:00:00']['open'].values[0]
    except IndexError:
        print('数据中没有9点的数据，请检查')
        print(x)
        return
    new['HIGH'] = x['high'].max()
    new['LOW'] = x['low'].min()
    new['CLOSE'] = x['close'].iloc[-1]
    new['VOLUME'] = x['volume'].sum()
    new['AMT'] = x['amount'].sum()
    new['OI'] = x['position'].iloc[-1]
    return new


for c in cmd_list:
    print('调整%s的量价数据' % c)
    queryArgs = {'wind_code': c}
    projectionFields = ['date', 'wind_code']
    res = list(fut_coll.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(1))
    if not res:
        queryArgs = {'wind_code': c, 'frequency': '1min'}
    else:
        dt_last = res[0]['date']
        start_dt = dt_last + timedelta(days=1)
        start_dt = start_dt.replace(hour=8, minute=0)
        queryArgs = {'wind_code': c, 'frequency': '1min', 'date_time': {'$gte': start_dt}}
    projectionFields = ['date_time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'position']
    res = min_coll.find(queryArgs, projectionFields).sort('date_time', pymongo.ASCENDING)
    df_res = pd.DataFrame.from_records(res, index='date_time')
    df_res.drop(columns='_id', inplace=True)
    df_res.dropna(how='all', subset=['open', 'high', 'low', 'close'], inplace=True)
    dt = []
    for i in df_res.index:
        if i.hour >= 8:
            dt.append(i.replace(hour=0, minute=0, second=0))
        else:
            dt.append((i - timedelta(days=1)).replace(hour=0, minute=0, second=0))
    df_res['date'] = dt
    df_res['time'] = [i.strftime('%H:%M:%S') for i in df_res.index]
    df_res.sort_index(ascending=True, inplace=True)
    df_group = df_res.groupby(by='date')
    new = df_group.apply(func=newK)
    new['wind_code'] = c
    new.sort_values(by='date', ascending=True, inplace=True)
    new.drop(new.index[-1], axis=0, inplace=True)
    new['date'] = new.index
    new_dict = new.to_dict(orient='record')
    for d in new_dict:
        d.update({'update_time': datetime.now()})
        fut_coll.insert_one(d)

def newKTB(x):
    new = pd.Series()
    try:
        new['OPEN'] = x[x['time'] == '09:00:00']['OPEN'].values[0]
    except IndexError:
        print('数据中没有9点的数据，请检查')
        print(x)
        return
    new['HIGH'] = x['HIGH'].max()
    new['LOW'] = x['LOW'].min()
    new['CLOSE'] = x['CLOSE'].iloc[-1]
    new['VOLUME'] = x['VOLUME'].sum()
    new['OI'] = x['OI'].iloc[-1]
    return new


for c in tb_list:
    print('调整%s的量价数据' % c)
    queryArgs = {'tb_code': c}
    projectionFields = ['date', 'tb_code']
    res = list(fut_coll.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(1))
    if not res:
        queryArgs = {'tb_code': c, 'frequency': '1min'}
    else:
        dt_last = res[0]['date']
        start_dt = dt_last + timedelta(days=1)
        start_dt = start_dt.replace(hour=8, minute=0)
        queryArgs = {'tb_code': c, 'frequency': '1min', 'date_time': {'$gte': start_dt}}
    projectionFields = ['date_time', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'OI']
    res = min_coll.find(queryArgs, projectionFields).sort('date_time', pymongo.ASCENDING)
    df_res = pd.DataFrame.from_records(res, index='date_time')
    df_res.drop(columns='_id', inplace=True)
    dt = []
    for i in df_res.index:
        if i.hour >= 8:
            dt.append(i.replace(hour=0, minute=0, second=0))
        else:
            dt.append((i - timedelta(days=1)).replace(hour=0, minute=0, second=0))
    df_res['date'] = dt
    df_res['time'] = [i.strftime('%H:%M:%S') for i in df_res.index]
    df_res.sort_index(ascending=True, inplace=True)
    df_group = df_res.groupby(by='date')
    new = df_group.apply(func=newKTB)
    new['tb_code'] = c
    new.sort_values(by='date', ascending=True, inplace=True)
    new.drop(new.index[-1], axis=0, inplace=True)
    new['date'] = new.index
    new_dict = new.to_dict(orient='record')
    for d in new_dict:
        d.update({'update_time': datetime.now()})
        fut_coll.insert_one(d)













