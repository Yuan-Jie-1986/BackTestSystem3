"""
对于路透的外盘分钟数据，重新组成主力合约
根据近1到近6的合约的VOLUME来进行选择
"""

import pymongo
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
import os
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 12)
pd.set_option('display.width', 200)

raw_path = 'E:\CBNB\BackTestSystem3\BT_DATA\DERIVED_DATA\ThomsonRetuers'

save_path = 'E:\CBNB\BackTestSystem3\BT_DATA\DERIVED_DATA\ThomsonRetuers'

asset = ['LCO', 'CL', 'GC', 'SI']

for a in asset:

    asset_path = os.path.join(raw_path, a + '_CST')

    df_raw_1 = pd.read_csv(os.path.join(asset_path, a + 'c1.csv'), index_col=0, parse_dates=True)
    df_vol_1 = df_raw_1[['VOLUME']].copy()
    df_vol_1.rename(columns={'VOLUME': a + 'c1'}, inplace=True)

    df_raw_2 = pd.read_csv(os.path.join(asset_path, a + 'c2.csv'), index_col=0, parse_dates=True)
    df_vol_2 = df_raw_2[['VOLUME']].copy()
    df_vol_2.rename(columns={'VOLUME': a + 'c2'}, inplace=True)

    df_raw_3 = pd.read_csv(os.path.join(asset_path, a + 'c3.csv'), index_col=0, parse_dates=True)
    df_vol_3 = df_raw_3[['VOLUME']].copy()
    df_vol_3.rename(columns={'VOLUME': a + 'c3'}, inplace=True)

    df_raw_4 = pd.read_csv(os.path.join(asset_path, a + 'c4.csv'), index_col=0, parse_dates=True)
    df_vol_4 = df_raw_4[['VOLUME']].copy()
    df_vol_4.rename(columns={'VOLUME': a + 'c4'}, inplace=True)

    df_raw_5 = pd.read_csv(os.path.join(asset_path, a + 'c5.csv'), index_col=0, parse_dates=True)
    df_vol_5 = df_raw_5[['VOLUME']].copy()
    df_vol_5.rename(columns={'VOLUME': a + 'c5'}, inplace=True)

    df_raw_6 = pd.read_csv(os.path.join(asset_path, a + 'c6.csv'), index_col=0, parse_dates=True)
    df_vol_6 = df_raw_6[['VOLUME']].copy()
    df_vol_6.rename(columns={'VOLUME': a + 'c6'}, inplace=True)

    df_vol = pd.concat((df_vol_1, df_vol_2, df_vol_3, df_vol_4, df_vol_5, df_vol_6), axis=1, join='outer')
    df_vol['day'] = [d.strftime('%Y%m%d') for d in df_vol.index]
    df_vol_daily = df_vol.groupby('day').sum()

    vol_max = df_vol_daily.max(axis=1)
    idx_max = df_vol_daily.isin(vol_max)

    df_total = pd.DataFrame()
    for c in idx_max.columns:
        df_c = idx_max[c]
        df_c = df_c[df_c]
        if df_c.empty:
            continue
        if 'c1' in c:
            df_raw_1['day'] = [d.strftime('%Y%m%d') for d in df_raw_1.index]
            df_temp = df_raw_1.loc[df_raw_1['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))
        if 'c2' in c:
            df_raw_2['day'] = [d.strftime('%Y%m%d') for d in df_raw_2.index]
            df_temp = df_raw_2.loc[df_raw_2['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))
        if 'c3' in c:
            df_raw_3['day'] = [d.strftime('%Y%m%d') for d in df_raw_3.index]
            df_temp = df_raw_3.loc[df_raw_3['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))
        if 'c4' in c:
            df_raw_4['day'] = [d.strftime('%Y%m%d') for d in df_raw_4.index]
            df_temp = df_raw_4.loc[df_raw_4['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))
        if 'c5' in c:
            df_raw_5['day'] = [d.strftime('%Y%m%d') for d in df_raw_5.index]
            df_temp = df_raw_5.loc[df_raw_5['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))
        if 'c6' in c:
            df_raw_6['day'] = [d.strftime('%Y%m%d') for d in df_raw_6.index]
            df_temp = df_raw_6.loc[df_raw_6['day'].isin(df_c.index)]
            df_temp = df_temp.drop(columns='day')
            df_temp['CONTRACT'] = c
            df_total = pd.concat((df_total, df_temp))

    df_total.sort_index(ascending=True, inplace=True)

    df_total.to_csv(os.path.join(asset_path, a + '_m1.csv'), encoding='GBK')




    # print(df_vol_daily == df_vol_daily.max(axis=1))
    # df_vol_daily.plot()
    # plt.show()






# def find_c1_contract(x):
#     '''寻找近月合约'''
#     new = x.sort_values(by='last_trade_date', ascending=True)
#     return new.iloc[0]
#
# def find_main_contract(x):
#     """寻找主力合约"""
#     # x_oi = x['OI']
#     # # x_oi.fillna(0, inplace=True)
#     # # if (x_oi < 1e4).all():
#     # #     return
#     # # if (x_oi == 0).all() or (x_oi < 1e4).all():
#     # #     new = x.sort_values(by='last_trade_date', ascending=True)
#     # #     return new.iloc[0]
#     # # else:
#     new = x.sort_values(by='OI', ascending=False)
#     return new.iloc[0]
#
# def find_submain_contract(x):
#     '''寻找次主力合约'''
#     main_contract = find_main_contract(x)
#     print(main_contract)
#     new = x.drop(main_contract.name, axis=0)
#     new = new[new['last_trade_date'] > main_contract['last_trade_date']]
#     new.sort_values(by='OI', ascending=False, inplace=True)
#     print(new)
#     print('date' in new)
#     if 'date' not in new or new.empty:
#         # raise Exception()
#
#
#         pass
#     return new.iloc[0]
#
# path = 'temp'
#
# for cmd in cmd_list:
#     print(cmd)
#     if cmd != 'M.DCE':
#         continue
#
#     ptn1 = re.compile('\w+(?=\.)')
#     res1 = ptn1.search(cmd).group()
#     ptn2 = re.compile('(?<=\.)\w+')
#     res2 = ptn2.search(cmd).group()
#
#     # 各合约信息
#     queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}}
#     projectionFields = ['wind_code', 'contract_issue_date', 'last_trade_date']
#     record = info_collection.find(queryArgs, projectionFields).sort('wind_code', pymongo.ASCENDING)
#     df_info = pd.DataFrame.from_records(record)
#     df_info.drop(columns='_id', inplace=True)
#
#     # 各合约量价
#     projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM']
#     record = future_collection.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
#     df = pd.DataFrame.from_records(record)
#     df.drop(columns='_id', inplace=True)
#
#     # 将两个dataframe合并
#     df = pd.merge(df, df_info, on='wind_code', how='left')
#
#     # 对于郑商所某些合约的处理
#     con1 = df['contract_issue_date'] > df['date']
#     df = df[~con1]
#     con2 = df['last_trade_date'] < df['date']
#     df = df[~con2]
#
#     df['fake_code'] = df['wind_code'].apply(func=seize_code)
#     # wind_code_list = set(df['wind_code'].values.flatten())
#     # df_total = pd.DataFrame()
#     # for w in wind_code_list:
#     #     df_w = df.loc[df['wind_code'] == w].copy()
#     #     df_w.sort_values(by='date', ascending=True, inplace=True)
#     #     df_w['OI_10_MA'] = df_w[['OI']].rolling(window=10).mean()
#     #     df_total = pd.concat((df_total, df_w))
#     # df_total.sort_values(by='date', ascending=True, inplace=True)
#
#     df.drop_duplicates(
#         subset=['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM', 'fake_code'], inplace=True)
#
#     df.drop(columns='fake_code', inplace=True)
#     df_group = df.groupby(by='date')
#
#     # 近月合约
#     df_c1 = df_group.apply(func=find_c1_contract)
#     # df_c1.drop(columns='OI_10_MA', inplace=True)
#     df_c1.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
#     df_temp = df_c1.shift(periods=1)
#     df_c1['switch_contract'] = df_c1['specific_contract'] != df_temp['specific_contract']
#
#     df_c1.to_csv(os.path.join(path, cmd + '_c1.csv'))
#
#     # 主力合约
#     df_main = df_group.apply(func=find_main_contract)
#     df_main.dropna(axis=0, how='all', inplace=True)
#     df_temp = df_main.shift(periods=1)
#     con = df_main['last_trade_date'] < df_temp['last_trade_date']
#     df_main[con] = np.nan
#     df_main.fillna(method='ffill', inplace=True)
#     df_main.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
#     df_temp = df_main.shift(periods=1)
#     df_main['switch_contract'] = df_main['specific_contract'] != df_temp['specific_contract']
#
#     df_main.to_csv(os.path.join(path, cmd + '_main.csv'))
#
#     # 次主力合约
#     df.to_csv('ddddddddddddd.csv')
#     df_submain = df_group.apply(func=find_submain_contract)
#     df_temp = df_submain.shift(periods=1)
#     con = df_submain['last_trade_date'] < df_temp['last_trade_date']
#     df_submain[con] = np.nan
#     df_submain.fillna(method='ffill', inplace=True)
#     df_submain.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
#     df_temp = df_submain.shift(periods=1)
#     df_submain['switch_contract'] = df_submain['specific_contract'] != df_temp['specific_contract']
#
#     df_submain.to_csv(os.path.join(path, cmd + '_submain.csv'))