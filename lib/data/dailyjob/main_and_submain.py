"""
根据万得的合约数据生成近1合约，主力和次主力合约
将生成的数据存到DerivDB数据表里
对于近一合约，以***.***_c1命名
对于主力合约, 以***.**_m1命名
对于次主力合约，以***.**_m2命名
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
            'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'AU.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE', 'ZN.SHF',
            'PB.SHF', 'SN.SHF', 'EB.DCE', 'Y.DCE', 'P.DCE', 'CF.CZC', 'OI.CZC']

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
    new = x.sort_values(by='OI', ascending=False)
    # print(new.iloc[0])
    # print(new.iloc[0][['wind_code', 'last_trade_date']])
    return new.iloc[0][['wind_code', 'last_trade_date']]

def find_submain_contract(x):
    '''寻找次主力合约'''
    main_contract = find_main_contract(x)
    new = x.drop(main_contract.name, axis=0)
    new = new[new['last_trade_date'] > main_contract['last_trade_date']]
    new.sort_values(by='OI', ascending=False, inplace=True)

    # 某些主力合约之后就没有合约了，这种情况就返回nan
    if new.empty:
        return pd.Series(np.ones(2) * np.nan, index=['wind_code', 'last_trade_date'])

    if 'date' not in new:
        raise Exception('date字段不存在，请检查')

    return new.iloc[0][['wind_code', 'last_trade_date']]




for cmd in cmd_list:
    print(cmd)

    cmd_c1 = cmd + '_c1'
    cmd_m1 = cmd + '_m1'
    cmd_m2 = cmd + '_m2'

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

    for nm in [cmd_c1, cmd_m1, cmd_m2]:

        print(nm)

        queryArgs = {'name': nm}
        projectionFields = ['date']
        res = list(deriv_collection.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(2))

        # 各合约量价
        if not res:
            queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}}
        else:
            # 这里的日期需要往前选一天，这样可以来判断第二天的主力合约和次主力合约
            dt_start = res[1]['date']
            queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}, 'date': {'$gte': dt_start}}

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

        df.drop_duplicates(
            subset=['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM', 'fake_code'], inplace=True)

        df.drop(columns='fake_code', inplace=True)

        df_group = df.groupby(by='date')

        if nm == cmd_c1:
            # 近月合约
            df_c1 = df_group.apply(func=find_c1_contract)
            df_c1.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
            df_temp = df_c1.shift(periods=1)
            df_c1['switch_contract'] = df_c1['specific_contract'] != df_temp['specific_contract']
            df_c1['remain_days'] = df_c1['last_trade_date'] - df_c1['date']
            df_c1['remain_days'] = [rd.days for rd in df_c1['remain_days']]
            df_c1['name'] = nm
            dict_c1 = df_c1.to_dict(orient='records')
            # 如果是第一次生成
            if not res:
                for d in dict_c1:
                    d.update({'update_time': datetime.now()})
                    deriv_collection.insert_one(d)
            # 如果是更新
            else:
                for d in dict_c1:
                    if not deriv_collection.find_one({'name': nm, 'date': d['date']}):
                        d.update({'update_time': datetime.now()})
                        deriv_collection.insert_one(d)
                    else:
                        pass

            # df_c1.to_csv(os.path.join(path, cmd + '_c1.csv'))

        elif nm == cmd_m1:
            # 主力合约，主力合约只能往前推，不能往回
            # 先找到持仓量最大的合约，然后再判断其到期日是否出现倒退，如果出现了，就赋为nan然后填充
            main_code = df_group.apply(func=find_main_contract)
            main_code = main_code.shift(periods=1)
            main_code.dropna(axis=0, how='all', inplace=True)

            # 对于某些不活跃的品种，通常是到期的时候仍然是主力，这样处理后，到期后的一天没有主力合约，比如BU在2014-9-16
            main_code = main_code.loc[main_code.index <= main_code['last_trade_date']]

            main_code['date_numeric'] = pd.to_numeric(main_code['last_trade_date'])
            main_code['latest_trade_date'] = main_code['date_numeric'].expanding().max()
            main_code['latest_trade_date'] = pd.to_datetime(main_code['latest_trade_date'])
            main_code[main_code['last_trade_date'] != main_code['latest_trade_date']] = np.nan
            main_code.drop(columns=['latest_trade_date', 'date_numeric'], inplace=True)
            main_code.fillna(method='ffill', inplace=True)

            # 根据得到的主力合约来找到相应的量价数据
            df_main = pd.merge(main_code, df, how='left', on=['date', 'wind_code', 'last_trade_date'])
            df_main.index = df_main['date']
            # df_main.drop(columns='date', inplace=True)
            # df_main.dropna(axis=0, how='all', inplace=True)

            # 增加switch_contract字段
            df_main.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
            df_temp = df_main.shift(periods=1)
            df_main['switch_contract'] = df_main['specific_contract'] != df_temp['specific_contract']
            df_main['remain_days'] = df_main['last_trade_date'] - df_main['date']
            df_main['remain_days'] = [rd.days for rd in df_main['remain_days']]
            df_main['name'] = nm
            dict_main = df_main.to_dict(orient='records')

            # 如果是第一次生成
            if not res:
                for d in dict_main:
                    d.update({'update_time': datetime.now()})
                    deriv_collection.insert_one(d)
            # 如果是更新
            else:
                for d in dict_main:
                    if not deriv_collection.find_one({'name': nm, 'date': d['date']}):
                        d.update({'update_time': datetime.now()})
                        deriv_collection.insert_one(d)
                    else:
                        pass

            # df_main.to_csv(os.path.join(path, cmd + '_m1.csv'))

        elif nm == cmd_m2:

            submain_code = df_group.apply(func=find_submain_contract)
            submain_code = submain_code.shift(periods=1)
            submain_code.dropna(axis=0, how='all', inplace=True)
            submain_code['date_numeric'] = pd.to_numeric(submain_code['last_trade_date'])
            submain_code['latest_trade_date'] = submain_code['date_numeric'].expanding().max()
            submain_code['latest_trade_date'] = pd.to_datetime(submain_code['latest_trade_date'])
            submain_code[submain_code['last_trade_date'] != submain_code['latest_trade_date']] = np.nan
            submain_code.drop(columns=['latest_trade_date', 'date_numeric'], inplace=True)
            submain_code.fillna(method='ffill', inplace=True)

            df_submain = pd.merge(submain_code, df, how='left', on=['date', 'wind_code', 'last_trade_date'])
            df_submain.index = df_submain['date']
            df_submain.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
            df_temp = df_submain.shift(periods=1)
            df_submain['switch_contract'] = df_submain['specific_contract'] != df_temp['specific_contract']
            df_submain['remain_days'] = df_submain['last_trade_date'] - df_submain['date']
            df_submain['remain_days'] = [rd.days for rd in df_submain['remain_days']]
            # df_submain = df_submain.reindex(df_main.index)
            # df_submain['date'] = df_submain.index
            df_submain['name'] = nm
            dict_submain = df_submain.to_dict(orient='records')

            # 如果是第一次生成
            if not res:
                for d in dict_submain:
                    d.update({'update_time': datetime.now()})
                    deriv_collection.insert_one(d)
            # 如果是更新
            else:
                for d in dict_submain:
                    if not deriv_collection.find_one({'name': nm, 'date': d['date']}):
                        d.update({'update_time': datetime.now()})
                        deriv_collection.insert_one(d)
                    else:
                        pass

            # df_submain.to_csv(os.path.join(path, cmd + '_m2.csv'))

        else:
            pass