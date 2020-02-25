#coding=utf-8
"""
将路透的数据根据各交易所的交易时间转成北京时间东八区
"""

import pandas as pd
import os
from datetime import datetime, timedelta, timezone


raw_path = 'E:\CBNB\BackTestSystem3\BT_DATA\RAW_DATA\FuturesMinMD'

save_path = 'E:\CBNB\BackTestSystem3\BT_DATA\DERIVED_DATA\ThomsonRetuers'

asset_list = ['LCO', 'CL', 'GC', 'SI']

for asset in asset_list:

    asset_path = os.path.join(raw_path, asset)

    contract_list = os.listdir(asset_path)

    asset_save_path = os.path.join(save_path, asset + '_CST')

    if not os.path.exists(asset_save_path):
        os.makedirs(asset_save_path)

    for contract in contract_list:
        print(contract)
        contract_path = os.path.join(asset_path, contract)
        df_contract = pd.read_csv(contract_path, index_col=0, parse_dates=True)
        df_contract = df_contract.tz_localize('UTC')
        df_contract = df_contract.tz_convert('Asia/Shanghai')
        # df_contract.index = [dt.astimezone(timezone(timedelta(hours=-8))).strftime('%Y-%m-%d %H:%M:%S') for dt in df_contract.index.to_pydatetime()]
        df_contract = df_contract.tz_localize(None)
        df_contract.to_csv(os.path.join(asset_save_path, contract), encoding='GBK')