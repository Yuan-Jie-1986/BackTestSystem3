"""
生成主力和次主力合约
"""

import pymongo
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
import os

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
info_collection = db['Information']

def seize_code(full_code):
    ptn1 = re.compile('[A-za-z]+(?=\d+\.)')
    res1 = ptn1.search(full_code).group()
    ptn2 = re.compile('(?<=([A-Za-z]))\d+(?=\.)')
    res2 = ptn2.search(full_code).group()
    ptn3 = re.compile('(?<=\d\.)[A-Za-z]+')
    res3 = ptn3.search(full_code).group()
    return res1 + res2[-3:] + '.' + res3

def find_c1_contract(x):
    '''寻找近月合约'''
    new = x.sort_values(by='last_trade_date', ascending=True)
    return new.iloc[0]

def find_main_contract(x):
    """寻找主力合约"""
    # x_oi = x['OI']
    # # x_oi.fillna(0, inplace=True)
    # # if (x_oi < 1e4).all():
    # #     return
    # # if (x_oi == 0).all() or (x_oi < 1e4).all():
    # #     new = x.sort_values(by='last_trade_date', ascending=True)
    # #     return new.iloc[0]
    # # else:
    new = x.sort_values(by='OI', ascending=False)
    return new.iloc[0]

def find_submain_contract(x):
    '''寻找次主力合约'''
    main_contract = find_main_contract(x)
    print(main_contract)
    new = x.drop(main_contract.name, axis=0)
    new = new[new['last_trade_date'] > main_contract['last_trade_date']]
    new.sort_values(by='OI', ascending=False, inplace=True)
    print(new)
    print('date' in new)
    if 'date' not in new or new.empty:
        # raise Exception()


        pass
    return new.iloc[0]

path = 'temp'

for cmd in cmd_list:
    print(cmd)
    if cmd != 'M.DCE':
        continue

    ptn1 = re.compile('\w+(?=\.)')
    res1 = ptn1.search(cmd).group()
    ptn2 = re.compile('(?<=\.)\w+')
    res2 = ptn2.search(cmd).group()

    # 各合约信息
    queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}}
    projectionFields = ['wind_code', 'contract_issue_date', 'last_trade_date']
    record = info_collection.find(queryArgs, projectionFields).sort('wind_code', pymongo.ASCENDING)
    df_info = pd.DataFrame.from_records(record)
    df_info.drop(columns='_id', inplace=True)

    # 各合约量价
    projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM']
    record = future_collection.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(record)
    df.drop(columns='_id', inplace=True)

    # 将两个dataframe合并
    df = pd.merge(df, df_info, on='wind_code', how='left')

    # 对于郑商所某些合约的处理
    con1 = df['contract_issue_date'] > df['date']
    df = df[~con1]
    con2 = df['last_trade_date'] < df['date']
    df = df[~con2]

    df['fake_code'] = df['wind_code'].apply(func=seize_code)
    # wind_code_list = set(df['wind_code'].values.flatten())
    # df_total = pd.DataFrame()
    # for w in wind_code_list:
    #     df_w = df.loc[df['wind_code'] == w].copy()
    #     df_w.sort_values(by='date', ascending=True, inplace=True)
    #     df_w['OI_10_MA'] = df_w[['OI']].rolling(window=10).mean()
    #     df_total = pd.concat((df_total, df_w))
    # df_total.sort_values(by='date', ascending=True, inplace=True)

    df.drop_duplicates(
        subset=['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM', 'fake_code'], inplace=True)

    df.drop(columns='fake_code', inplace=True)
    df_group = df.groupby(by='date')

    # 近月合约
    df_c1 = df_group.apply(func=find_c1_contract)
    # df_c1.drop(columns='OI_10_MA', inplace=True)
    df_c1.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
    df_temp = df_c1.shift(periods=1)
    df_c1['switch_contract'] = df_c1['specific_contract'] != df_temp['specific_contract']

    df_c1.to_csv(os.path.join(path, cmd + '_c1.csv'))

    # 主力合约
    df_main = df_group.apply(func=find_main_contract)
    df_main.dropna(axis=0, how='all', inplace=True)
    df_temp = df_main.shift(periods=1)
    con = df_main['last_trade_date'] < df_temp['last_trade_date']
    df_main[con] = np.nan
    df_main.fillna(method='ffill', inplace=True)
    df_main.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
    df_temp = df_main.shift(periods=1)
    df_main['switch_contract'] = df_main['specific_contract'] != df_temp['specific_contract']

    df_main.to_csv(os.path.join(path, cmd + '_main.csv'))

    # 次主力合约
    df.to_csv('ddddddddddddd.csv')
    df_submain = df_group.apply(func=find_submain_contract)
    df_temp = df_submain.shift(periods=1)
    con = df_submain['last_trade_date'] < df_temp['last_trade_date']
    df_submain[con] = np.nan
    df_submain.fillna(method='ffill', inplace=True)
    df_submain.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
    df_temp = df_submain.shift(periods=1)
    df_submain['switch_contract'] = df_submain['specific_contract'] != df_temp['specific_contract']

    df_submain.to_csv(os.path.join(path, cmd + '_submain.csv'))