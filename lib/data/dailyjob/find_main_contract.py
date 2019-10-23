
"""
该脚本是为了从wind的主力合约中寻找到具体是哪个合约，并记录下是否切换了合约以及距离到期的天数
"""

import pymongo
import pandas as pd
import re
import numpy as np
from datetime import datetime

# dataframe的输出格式
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)

cmd_list = ['M.DCE', 'L.DCE', 'PP.DCE', 'I.DCE', 'J.DCE', 'JM.DCE', 'C.DCE', 'RB.SHF', 'BU.SHF', 'RU.SHF', 'NI.SHF',
            'HC.SHF', 'TA.CZC', 'MA.CZC', 'AP.CZC', 'ZC.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE', 'EG.DCE', 'SP.SHF',
            'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'AU.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE', 'ZN.SHF',
            'PB.SHF', 'SN.SHF']

conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
futurs_coll = db['FuturesMD']
info_coll = db['Information']

for cmd in cmd_list:
    print(cmd)
    ptn_1 = re.compile('\w+(?=\.)')
    res_1 = ptn_1.search(cmd).group()
    ptn_2 = re.compile('(?<=\.)\w+')
    res_2 = ptn_2.search(cmd).group()

    queryArgs = {'wind_code': cmd}
    projectionFields = ['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
    res = futurs_coll.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(res, index='date')
    df.drop(columns=['_id'], inplace=True)
    ctr_dict = {}
    for dt in df.index:
        # 根据主力合约的日期寻找与主力日期相同的量价合约
        queryArgs = {'date': dt, 'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}}
        projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
        res_dt = futurs_coll.find(queryArgs, projectionFields).sort('OI', pymongo.DESCENDING).limit(6)
        dict_cmd = df.loc[dt].to_dict()

        keys_not_nan = [k for k in dict_cmd.keys() if ~np.isnan(dict_cmd[k])]

        for r in res_dt:

            # if dt == datetime(2000, 8, 4):
            #     print(dict_cmd)
            #     print(r)
            #     print(dict_cmd['OPEN'] == r['OPEN'] and dict_cmd['CLOSE'] == r['CLOSE'] and dict_cmd['HIGH'] == r['HIGH'] \
            #     and dict_cmd['LOW'] == r['LOW'] and dict_cmd['VOLUME'] == r['VOLUME'] and dict_cmd['OI'] == r['OI'])

            isSame = True
            for k in keys_not_nan:
                if dict_cmd[k] != r[k]:
                    isSame = False
                    break
            if isSame:
                ctr_dict[dt] = {}
                ctr_dict[dt]['wind_code'] = r['wind_code']
                break
            else:
                continue


            # if dict_cmd['OPEN'] == r['OPEN'] and dict_cmd['CLOSE'] == r['CLOSE'] and dict_cmd['HIGH'] == r['HIGH'] \
            #         and dict_cmd['LOW'] == r['LOW'] and dict_cmd['VOLUME'] == r['VOLUME'] and dict_cmd['OI'] == r['OI']:
            #
            #     ctr_dict[dt] = {}
            #     ctr_dict[dt]['wind_code'] = r['wind_code']
            #     # ctr_dict[dt].update(dict_cmd)
            #     break
    ctr_df = pd.DataFrame.from_dict(ctr_dict, orient='index').sort_index()
    ctr_df['last_wind_code'] = ctr_df['wind_code'].shift(1)
    ctr_df['switch_contract'] = ctr_df['last_wind_code'] != ctr_df['wind_code']
    ctr_df.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
    ctr_df.drop(columns=['last_wind_code'], inplace=True)

    queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}}
    projectionFields = ['wind_code', 'last_trade_date']
    info_res = info_coll.find(queryArgs, projectionFields)
    info_df = pd.DataFrame.from_records(info_res)
    info_df.drop(columns='_id', inplace=True)

    new_df = pd.merge(left=ctr_df, right=info_df, how='left', right_on='wind_code', left_on='specific_contract')
    new_df.index = ctr_df.index
    new_df['current_date'] = new_df.index
    new_df['remain_days'] = new_df['last_trade_date'] - new_df['current_date']
    new_df['remain_days'] = [rd.days for rd in new_df['remain_days']]
    new_df.drop(columns=['wind_code', 'current_date', 'last_trade_date'], inplace=True)

    record_dict = new_df.to_dict(orient='index')

    for k in record_dict:
        queryArgs = {'date': k, 'wind_code': cmd, '$or': [{'specific_contract': {'$exists': False}},
                                                           {'switch_contract': {'$exists': False}},
                                                           {'remain_days': {'$exists': False}}]}
        # projectionFields = ['date', 'wind_code', 'CLOSE']
        res_update = futurs_coll.update_many(queryArgs, {'$set': record_dict[k]})
        if res_update.matched_count == 0:
            continue
        if res_update.matched_count > 1:
            print(k, cmd)
            raise Exception(u'数据库中有重复')

    # 检查数据中是否有没有标注的日期
    queryArgs = {'wind_code': cmd, '$or': [{'specific_contract': {'$exists': False}},
                                           {'switch_contract': {'$exists': False}},
                                           {'remain_days': {'$exists': False}}]}
    projectionFields = ['date', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME', 'OI']
    res = futurs_coll.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(res)
    if not df.empty:
        print('标注主力合约的时候有遗漏，请检查')
        print(df)










