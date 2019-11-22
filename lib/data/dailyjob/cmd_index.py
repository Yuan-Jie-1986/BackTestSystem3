
"""
根据各合约的数据生成各品种的指数
"""

import pymongo
import pandas as pd
import re
from datetime import datetime, timedelta

cmd_list = ['L.DCE', 'PP.DCE', 'I.DCE', 'J.DCE', 'JM.DCE', 'M.DCE', 'C.DCE', 'RB.SHF', 'BU.SHF', 'RU.SHF', 'NI.SHF',
            'HC.SHF', 'TA.CZC', 'MA.CZC', 'AP.CZC', 'ZC.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE', 'EG.DCE', 'SP.SHF',
            'FG.CZC', 'V.DCE', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'AU.SHF', 'FU.SHF', 'IF.CFE', 'IH.CFE', 'IC.CFE', 'ZN.SHF',
            'PB.SHF', 'SN.SHF', 'EB.DCE']

conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
future_collection = db['FuturesMD']
deriv_collection = db['DerivDB']


def reconstruction(x):
    new = pd.Series()
    new['CLOSE'] = (x['CLOSE'] * x['OI']).sum() / x['OI'].sum()
    new['OPEN'] = (x['OPEN'] * x['OI']).sum() / x['OI'].sum()
    new['HIGH'] = (x['HIGH'] * x['OI']).sum() / x['OI'].sum()
    new['LOW'] = (x['LOW'] * x['OI']).sum() / x['OI'].sum()
    new['SETTLE'] = (x['SETTLE'] * x['OI']).sum() / x['OI'].sum()
    new['VOLUME'] = x['VOLUME'].sum()
    new['OI'] = x['OI'].sum()
    new['DEALNUM'] = x['DEALNUM'].sum()
    new['contract_count'] = x['wind_code'].count()
    return new

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
    ptn_1 = re.compile('\w+(?=\.)')
    res_1 = ptn_1.search(cmd).group()
    ptn_2 = re.compile('(?<=\.)\w+')
    res_2 = ptn_2.search(cmd).group()

    queryArgs = {'name': '%s888.%s' % (res_1, res_2)}
    projectionFields = ['name', 'date']
    res = list(deriv_collection.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(1))
    if not res:
        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}}
    else:
        start_date = res[0]['date'] + timedelta(1)
        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}, 'date': {'$gte': start_date}}
    projectionFields = ['date', 'wind_code', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM']
    records = future_collection.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(records)
    if df.empty:
        continue
    df.drop(columns='_id', inplace=True)
    df['fake_code'] = df['wind_code'].apply(func=seize_code)
    df.drop_duplicates(
        subset=['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'SETTLE', 'VOLUME', 'OI', 'DEALNUM', 'fake_code'], inplace=True)
    df.drop(columns='fake_code', inplace=True)
    df_group = df.groupby(by='date')
    df_index = df_group.apply(func=reconstruction)
    df_index['name'] = '%s888.%s' % (res_1, res_2)
    df_index['date'] = df_index.index
    dict_index = df_index.to_dict(orient='records')
    for d in dict_index:
        d.update({'update_time': datetime.now()})
        deriv_collection.insert_one(d)








