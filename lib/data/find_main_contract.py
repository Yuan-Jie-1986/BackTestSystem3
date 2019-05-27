#coding=utf-8

"""
该脚本是为了从wind的主力合约中寻找到具体是哪个合约，并
"""

import pymongo
import pandas as pd
import re

cmd_list = ['L.DCE', 'PP.DCE', 'I.DCE', 'J.DCE', 'JM.DCE', 'M.DCE', 'C.DCE', 'RB.SHF', 'BU.SHF', 'RU.SHF', 'NI.SHF',
            'HC.SHF', 'TA.CZC', 'MA.CZC', 'AP.CZC', 'ZC.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE', 'EG.DCE', 'SP.SHF',
            'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE']

conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
futurs_coll = db['FuturesMD']

for cmd in cmd_list:
    print cmd
    ptn_1 = re.compile('\w+(?=\.)')
    res_1 = ptn_1.search(cmd).group()
    ptn_2 = re.compile('(?<=\.)\w+')
    res_2 = ptn_2.search(cmd).group()

    querayArgs = {'wind_code': cmd}
    projectionFields = ['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
    res = futurs_coll.find(querayArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(res, index='date')
    df.drop(columns=['_id'], inplace=True)
    ctr_dict = {}
    for dt in df.index:
        querayArgs = {'date': dt, 'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}}
        projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
        res_dt = futurs_coll.find(querayArgs, projectionFields).sort('VOLUME', pymongo.DESCENDING).limit(3)
        for r in res_dt:
            dict_cmd = df.loc[dt].to_dict()
            if dict_cmd['OPEN'] == r['OPEN'] and dict_cmd['CLOSE'] == r['CLOSE'] and dict_cmd['HIGH'] == r['HIGH'] \
                and dict_cmd['LOW'] == r['LOW'] and dict_cmd['VOLUME'] == r['VOLUME'] and dict_cmd['OI'] == r['OI']:

                ctr_dict[dt] = {}
                ctr_dict[dt]['wind_code'] = r['wind_code']
                # ctr_dict[dt].update(dict_cmd)
                break
    ctr_df = pd.DataFrame.from_dict(ctr_dict, orient='index').sort_index()
    ctr_df['last_wind_code'] = ctr_df['wind_code'].shift(1)
    ctr_df['switch_contract'] = ctr_df['last_wind_code'] != ctr_df['wind_code']
    ctr_df.rename(columns={'wind_code': 'specific_contract'}, inplace=True)
    ctr_df.drop(columns=['last_wind_code'], inplace=True)
    record_dict = ctr_df.to_dict(orient='index')
    for k in record_dict:
        querayArgs = {'date': k, 'wind_code': cmd, 'specific_contract': {'$exists': False},
                      'switch_contract': {'$exists': False}}
        # projectionFields = ['date', 'wind_code', 'CLOSE']
        res_update = futurs_coll.update_many(querayArgs, {'$set': record_dict[k]})
        if res_update.matched_count == 0:
            continue
        if res_update.matched_count > 1:
            print k, cmd
            raise Exception(u'数据库中有重复')






