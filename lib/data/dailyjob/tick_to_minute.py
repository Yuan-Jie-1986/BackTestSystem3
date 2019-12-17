# coding=gbk

"""
Author: YuanJie
该脚本是用于将tick数据转换成分钟数据
"""

import sys
import re
import os
import pandas as pd
from datetime import datetime


pd.set_option('max_columns', 30)
pd.set_option('display.width', 150)
DATA_SOURCE = 'VNPY'
DATA_PATH = 'E:\CBNB\BackTestSystem3\BT_DATA\DERIVED_DATA'




#　以下是用来处理朱九界提供的tick数据

if DATA_SOURCE == 'JiuJie':

    field_c2e = {'交易日': 'date',
                 '合约代码': 'contract_code',
                 '交易所代码': 'exchange_code',
                 '合约在交易所的代码': 'code_in_exchange',
                 '最新价': 'last_price',
                 '上次结算价': 'previous_settlement',
                 '昨收盘': 'previous_close',
                 '昨持仓量': 'previous_open_interest',
                 '今开盘': 'open',
                 '最高价': 'high',
                 '最低价': 'low',
                 '数量': 'volume',
                 '成交金额': 'amount',
                 '持仓量': 'open_interest',
                 '今收盘': 'close',
                 '本次结算价': 'settlement',
                 '涨停板价': 'up_limit',
                 '跌停板价': 'down_limit',
                 '昨虚实度': 'previous_xushidu',
                 '今虚实度': 'xushidu',
                 '最后修改时间': 'time',
                 '最后修改毫秒': 'time_millisec',
                 '申买价一': 'bid_price_1',
                 '申买量一': 'bid_volume_1',
                 '申卖价一': 'ask_price_1',
                 '申卖量一': 'ask_volume_1',
                 '申买价二': 'bid_price_2',
                 '申买量二': 'bid_volume_2',
                 '申卖价二': 'ask_price_2',
                 '申卖量二': 'ask_volume_2',
                 '申买价三': 'bid_price_3',
                 '申买量三': 'bid_volume_3',
                 '申卖价三': 'ask_price_3',
                 '申卖量三': 'ask_volume_3',
                 '申买价四': 'bid_price_4',
                 '申买量四': 'bid_volume_4',
                 '申卖价四': 'ask_price_4',
                 '申卖量四': 'ask_volume_4',
                 '申买价五': 'bid_price_5',
                 '申买量五': 'bid_volume_5',
                 '申卖价五': 'ask_price_5',
                 '申卖量五': 'ask_volume_5',
                 '当日均价': 'average_price'}

    tick_path = 'F:\\高频\\商品CTP\\2010'

    for root, dirs, files in os.walk(tick_path):
        for name in files:
            if os.path.splitext(name)[1] == '.csv' and os.path.splitext(name)[0] == 'rb1005':

                df_tick = pd.read_csv(os.path.join(root, name), encoding='gbk')
                df_tick.rename(columns=field_c2e, inplace=True)
                df_tick = df_tick.loc[:, (df_tick != 0).any(axis=0)]  # 将数值全为0的项删除
                df_tick['date'] = [str(d) for d in df_tick['date']]
                df_tick['hour_min'] = [t[:-3] for t in df_tick['time']]
                df_tick['date_hour_min'] = df_tick['date'] + ' ' + df_tick['hour_min']
                df_grouped = df_tick.groupby(by='date_hour_min')
                df_open = df_grouped[['last_price']].apply(func=lambda x: x.iloc[0])
                df_open.rename(columns={'last_price': 'open'}, inplace=True)

                df_close = df_grouped[['last_price']].apply(func=lambda x: x.iloc[-1])
                df_close.rename(columns={'last_price': 'close'}, inplace=True)

                df_high = df_grouped[['last_price']].max()
                df_high.rename(columns={'last_price': 'high'}, inplace=True)

                df_low = df_grouped[['last_price']].min()
                df_low.rename(columns={'last_price': 'low'}, inplace=True)

                df_minute = pd.concat((df_open, df_high, df_low, df_close), axis=1)
                df_minute.index.rename('date_time', inplace=True)

                print(df_minute)
                print(df_tick.columns)

# 以下是处理VNPY的tick数据
elif DATA_SOURCE == 'VNPY':
    tick_path = 'F:\TickVNPY\DataCollect采集的期货全市场数据2018.6.27.~2018.11.29\Data'

    new_columns = ['LocalTime', 'InstrumentID', 'TradingDay', 'ActionDay', 'UpdateTime', 'UpdateMillisec', 'LastPrice',
                   'Volume', 'HighestPrice', 'LowestPrice', 'OpenPrice', 'ClosePrice', 'AveragePrice', 'AskPrice1',
                   'AskVolume1', 'BidPrice1', 'BidVolume1', 'UpperLimitPrice', 'LowerLimitPrice', 'OpenInterest',
                   'Turnover', 'PreClosePrice', 'PreOpenInterest', 'PreSettlementPrice']

    name_list = []
    for root, dirs, files in os.walk(tick_path):
        for name in files:
            if os.path.splitext(name)[1] == '.csv':
                name_list.append(os.path.splitext(name)[0])
    names = set(name_list)

    total = len(names)
    count = 1.

    ptn = re.compile('[A-Za-z]+(?=\d+)')
    for s in names:

        process_str = '【' + '-' * int(count / total * 100) + ' ' * (100 - int(count / total) * 100) + '已完成%5.2f%%】' % (
                    count / total * 100.)
        sys.stdout.write('\r' + process_str)
        sys.stdout.flush()

        category = ptn.search(s).group()
        category = category.upper()
        save_name = os.path.join(DATA_PATH, DATA_SOURCE, category, '%s.csv' % s)

        # 判断是否有该路径
        if not os.path.exists(os.path.join(DATA_PATH, DATA_SOURCE, category)):
            os.makedirs(os.path.join(DATA_PATH, DATA_SOURCE, category))

        df_s = pd.DataFrame()
        for root, dirs, files in os.walk(tick_path):
            for name in files:
                if os.path.splitext(name)[1] == '.csv' and os.path.splitext(name)[0] == s:

                    if os.path.exists(save_name):
                        df_exists = pd.read_csv(save_name, usecols=['date_time', 'TradingTime'], iterator=True, chunksize=1e6)
                        break
                        # print(df_exists)


                    df_csv = pd.read_csv(os.path.join(root, name), names=new_columns)


                    df_s = pd.concat((df_s, df_csv))
                    if df_s.memory_usage().sum() / (1024 ** 2) > 150:
                        break
            if df_s.memory_usage().sum() / (1024 ** 2) > 150:
                break
            if os.path.exists(save_name):
                df_exists = pd.read_csv(save_name, usecols=['date_time', 'TradingTime'], iterator=True, chunksize=1e6)
                count += 1
                break
        try:
            if df_s.empty:
                continue

            df_s = df_s.sort_values(by=['ActionDay', 'UpdateTime', 'UpdateMillisec'], ascending=True)

            df_s['ActionDay'] = [str(d) for d in df_s['ActionDay']]
            df_s['TradingTime'] = [t[:-3] for t in df_s['UpdateTime']]
            df_s['date_hour_min'] = df_s['ActionDay'] + ' ' + df_s['TradingTime']

            df_grouped = df_s.groupby(by='date_hour_min')

            df_open = df_grouped[['LastPrice']].apply(func=lambda x: x.iloc[0])
            df_open.rename(columns={'LastPrice': 'OpenPrice'}, inplace=True)

            df_close = df_grouped[['LastPrice']].apply(func=lambda x: x.iloc[-1])
            df_close.rename(columns={'LastPrice': 'CLosePrice'}, inplace=True)

            df_high = df_grouped[['LastPrice']].max()
            df_high.rename(columns={'LastPrice': 'HighPrice'}, inplace=True)

            df_low = df_grouped[['LastPrice']].min()
            df_low.rename(columns={'LastPrice': 'LowPrice'}, inplace=True)

            df_vol = df_grouped[['Volume']].apply(func=lambda x: x.iloc[-1])
            df_vol['preVol'] = df_vol['Volume'].shift(periods=1)
            df_vol['isNew'] = df_vol['Volume'] >= df_vol['preVol']

            df_vol['ChgVolume'] = df_vol['Volume'].diff(periods=1)
            df_vol.loc[~df_vol['isNew'], 'ChgVolume'] = df_vol.loc[~df_vol['isNew'], 'Volume']
            df_vol.loc[df_vol.index[0], 'ChgVolume'] = df_vol.loc[df_vol.index[0], 'Volume']
            df_vol.drop(['preVol', 'isNew'], axis=1, inplace=True)

            df_oi = df_grouped[['OpenInterest']].apply(func=lambda x: x.iloc[-1])
            df_oi['ChgOI'] = df_oi['OpenInterest'].diff(periods=1)
            df_oi.loc[df_oi.index[0], 'ChgOI'] = df_oi.loc[df_oi.index[0], 'OpenInterest']

            df_turnover = df_grouped[['Turnover']].apply(func=lambda x: x.iloc[-1])
            df_turnover['preTurnover'] = df_turnover['Turnover'].shift(periods=1)
            df_turnover['isNew'] = df_turnover['Turnover'] >= df_turnover['preTurnover']
            df_turnover['ChgTurnover'] = df_turnover['Turnover'].diff(periods=1)
            df_turnover.loc[~df_turnover['isNew'], 'ChgTurnover'] = df_turnover.loc[~df_turnover['isNew'], 'Turnover']
            df_turnover.loc[df_turnover.index[0], 'ChgTurnover'] = df_turnover.loc[df_turnover.index[0], 'Turnover']
            df_turnover.drop(['preTurnover', 'isNew'], axis=1, inplace=True)

            df_askvol = df_grouped[['AskVolume1']].sum()
            df_bidvol = df_grouped[['BidVolume1']].sum()


            df_minute = pd.concat((df_open, df_high, df_low, df_close, df_vol, df_oi, df_turnover, df_askvol, df_bidvol), axis=1)
            df_minute.index.rename('date_time', inplace=True)
            df_minute['TradingTime'] = [(datetime.strptime(tm, '%Y%m%d %H:%M')) for tm in df_minute.index]

            if not os.path.exists(os.path.join(DATA_PATH, DATA_SOURCE, category, '%s.csv' % s)):
                df_minute.to_csv(os.path.join(DATA_PATH, DATA_SOURCE, category, '%s.csv' % s))

            count += 1
        except Exception as e:
            print(s)
            print(df_s)
            raise Exception(e)


    sys.stdout.write('\n')
    sys.stdout.flush()











