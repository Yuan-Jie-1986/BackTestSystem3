
import pandas as pd
import os
from datetime import datetime, timedelta

asset = ['AG.SHF', 'AL.SHF', 'AU.SHF', 'BU.SHF', 'CU.SHF', 'EG.DCE', 'FG.CZC', 'FU.SHF', 'HC.SHF', 'I.DCE', 'J.DCE',
         'JM.DCE', 'L.DCE', 'MA.CZC', 'NI.SHF', 'PP.DCE', 'RB.SHF', 'RU.SHF', 'SC.INE', 'TA.CZC', 'V.DCE', 'ZC.CZC',
         'ZN.SHF']
raw_path = 'E:\CBNB\BackTestSystem3\BT_DATA\RAW_DATA\FuturesMinMD'

goal_path = 'E:\CBNB\BackTestSystem3\BT_DATA\DERIVED_DATA\Domestic'

def calcTradingDate(x):

    dt = datetime.strptime(x['date'], '%Y-%m-%d')
    tm = datetime.strptime(x['time'], '%H:%M:%S')

    hour = tm.hour
    weekday = dt.weekday()
    if hour >= 20 and weekday == 4:
        return datetime.strptime(x['date'], '%Y-%m-%d') + timedelta(days=3)
    elif hour >= 20 and weekday < 4:
        return datetime.strptime(x['date'], '%Y-%m-%d') + timedelta(days=1)
    elif hour < 8 and weekday == 5:
        return datetime.strptime(x['date'], '%Y-%m-%d') + timedelta(days=2)
    else:
        return datetime.strptime(x['date'], '%Y-%m-%d')

for a in asset:
    print(a)
    source_path = os.path.join(raw_path, a)
    file_list = os.listdir(source_path)
    save_path = os.path.join(goal_path, a)

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for f in file_list:
        df_raw = pd.read_csv(os.path.join(source_path, f), index_col=0, parse_dates=True)
        df_raw['trading_date'] = df_raw.apply(func=calcTradingDate, raw=False, axis=1)
        df_raw.to_csv(os.path.join(save_path, f), encoding='GBK')


