# coding=utf-8

"""
用于检查config.yaml文件里的数据最新更新时间
作者：YUANJIE
"""

import yaml
import pymongo
import pandas as pd
import re


pd.set_option('display.max_columns', 12)
pd.set_option('display.width', 200)

f = open('E:\CBNB\BackTestSystem3\lib\data\config.yaml', encoding='utf-8')
yaml_res = yaml.load(f, Loader=yaml.FullLoader)
host = yaml_res['host']
port = yaml_res['port']
usr = yaml_res['user']
pwd = yaml_res['pwd']
db = yaml_res['db_name']
conn = pymongo.MongoClient(host=host, port=port)
database = conn[db]
database.authenticate(name=usr, password=pwd)

collection_list = ['Information', 'FuturesMD', 'SpotMD', 'EDB', 'Inventory']

df_updatetime = pd.DataFrame()
for coll in collection_list:
    if coll == 'Information':
        df_coll = pd.DataFrame()
        source = yaml_res[coll][0]['source']
        cmd_list = yaml_res[coll][0]['cmd']
        for cmd in cmd_list:
            ptn1 = re.compile('\A[A-Z]+(?=\.)')
            res1 = ptn1.search(cmd).group()
            ptn2 = re.compile('(?<=\.)[A-Z]+')
            res2 = ptn2.search(cmd).group()
            # 如果品种属于中国的期货品种
            if res2 in ['SHF', 'CZC', 'DCE', 'CFE', 'INE']:
                queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res1, res2)}, 'source': source}
            # 如果品种是COMEX、NYMEX、ICE的合约，通常是品种+月份字母+年份数字+E(表示电子盘)+.交易所代码
            elif res2 in ['CMX', 'NYM', 'IPE']:
                queryArgs = {'wind_code': {'$regex': '\A%s[FGHJKMNOUVXZ]\d+E\.%s\Z' % (res1, res2)}, 'source': source}
            projectionField = ['wind_code', 'update_time', 'source']
            res = database[coll].find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            df_res['collection'] = coll
            df_coll = pd.concat((df_coll, df_res))
        df_coll.sort_values(by='update_time', ascending=True, inplace=True)
        df_updatetime = pd.concat((df_updatetime, df_coll))

    elif coll in ['SpotMD', 'EDB', 'FuturesMD', 'Inventory']:
        df_coll = pd.DataFrame()
        category = yaml_res[coll]
        for ct in category:
            source = ct['source']
            cmd = ct['cmd']
            if isinstance(cmd, list):
                for c in cmd:
                    queryArgs = {'$or': [{'wind_code': c}, {'tr_code': c}, {'commodity': c}], 'source': source}
                    if 'area' in ct.keys():
                        queryArgs.update({'area': ct['area']})
                    projectionField = ['date', 'wind_code', 'tr_code', 'commodity', 'update_time', 'source', 'area']
                    res = database[coll].find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(
                        1)
                    df_res = pd.DataFrame.from_records(res)
                    df_res.drop(columns='_id', inplace=True)
                    df_res['collection'] = coll
                    df_coll = pd.concat((df_coll, df_res), sort=False)

            else:
                queryArgs = {'$or': [{'wind_code': cmd}, {'tr_code': cmd}, {'commodity': cmd}], 'source': source}
                if 'area' in ct.keys():
                    queryArgs.update({'area': ct['area']})
                projectionField = ['date', 'wind_code', 'tr_code', 'commodity', 'update_time', 'source', 'area']
                res = database[coll].find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
                df_res = pd.DataFrame.from_records(res)
                df_res.drop(columns='_id', inplace=True)
                df_res['collection'] = coll
                df_coll = pd.concat((df_coll, df_res), sort=False)
        df_coll.sort_values(by='update_time', ascending=True, inplace=True)
        # print(df_coll)
        df_updatetime = pd.concat((df_updatetime, df_coll), sort=False)


asset_list = ['L.DCE', 'PP.DCE', 'I.DCE', 'J.DCE', 'JM.DCE', 'M.DCE', 'C.DCE', 'RB.SHF', 'BU.SHF', 'RU.SHF', 'NI.SHF',
              'HC.SHF', 'TA.CZC', 'MA.CZC', 'AP.CZC', 'ZC.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE', 'EG.DCE', 'SP.SHF',
              'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'AU.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE',
              'ZN.SHF', 'PB.SHF', 'SN.SHF', 'EB.DCE', 'Y.DCE', 'P.DCE', 'CF.CZC', 'OI.CZC']

# 检查指数是否生成
coll = database['DerivDB']
df_coll = pd.DataFrame()
for asset in asset_list:
    ptn1 = re.compile('\A[A-Z]+(?=\.)')
    res1 = ptn1.search(asset).group()
    ptn2 = re.compile('(?<=\.)[A-Z]+')
    res2 = ptn2.search(asset).group()
    queryArgs = {'name': '%s888.%s' % (res1, res2)}
    projectionField = ['name', 'update_time', 'date']
    res = coll.find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
    df_res = pd.DataFrame.from_records(res)
    df_res.drop(columns='_id', inplace=True)
    df_res['collection'] = 'DerivDB'
    df_coll = pd.concat((df_coll, df_res), sort=False)

    queryArgs = {'name': asset + '_m1'}
    projectionField = ['name', 'update_time', 'date']
    res = coll.find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
    df_res = pd.DataFrame.from_records(res)
    df_res.drop(columns='_id', inplace=True)
    df_res['collection'] = 'DerivDB'
    df_coll = pd.concat((df_coll, df_res), sort=False)

    queryArgs = {'name': asset + '_m2'}
    projectionField = ['name', 'update_time', 'date']
    res = coll.find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
    df_res = pd.DataFrame.from_records(res)
    df_res.drop(columns='_id', inplace=True)
    df_res['collection'] = 'DerivDB'
    df_coll = pd.concat((df_coll, df_res), sort=False)

df_coll.sort_values(by='update_time', ascending=True, inplace=True)
df_updatetime = pd.concat((df_updatetime, df_coll), sort=False)

coll = database['FuturesMD']
df_coll = pd.DataFrame()
for asset in asset_list:
    queryArgs = {'wind_code': asset, 'specific_contract': {'$exists': 1}, 'switch_contract': {'$exists': 1},
                 'remain_days': {'$exists': 1}}
    projectionField = ['wind_code', 'date', 'update_time', 'specific_contract', 'switch_contract', 'remain_days']
    res = coll.find(queryArgs, projectionField).sort('update_time', pymongo.DESCENDING).limit(1)
    df_res = pd.DataFrame.from_records(res)
    df_res.drop(columns='_id', inplace=True)
    df_res['collection'] = 'FuturesMD'
    df_coll = pd.concat((df_coll, df_res), sort=False)

df_coll.sort_values(by='update_time', ascending=True, inplace=True)
df_updatetime = pd.concat((df_updatetime, df_coll), sort=False)

df_updatetime.to_csv('CHECK_UPDATE.csv', encoding='GBK')










