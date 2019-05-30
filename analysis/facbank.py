#coding=utf-8
import numpy as np
import pandas as pd
import pymongo
from datetime import datetime


class FacGenerator(object):

    cmd_list = ['L.DCE', 'PP.DCE', 'MA.CZC', 'TA.CZC', 'BU.SHF', 'V.DCE', 'EG.DCE', 'ZC.CZC', 'JM.DCE', 'RB.SHF',
                'J.DCE', 'I.DCE', 'HC.SHF']

    cmd_all = ['L.DCE', 'PP.DCE', 'MA.CZC', 'TA.CZC', 'BU.SHF', 'V.DCE', 'EG.DCE', 'ZC.CZC', 'JM.DCE', 'RB.SHF',
               'J.DCE', 'I.DCE', 'HC.SHF', 'M.DCE', 'C.DCE', 'RU.SHF', 'NI.SHF', 'AP.CZC', 'SR.CZC', 'RM.CZC', 'SC.INE',
               'SP.SHF', 'FG.CZC', 'CU.SHF', 'AL.SHF', 'AG.SHF', 'FU.SHF']

    def __init__(self, start_date, end_date):
        conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
        db = conn['CBNB']
        db.authenticate(name='yuanjie', password='yuanjie')
        self.futures_coll = db['FuturesMD']

        # 高开低收量仓的dataframe

        self.cls = pd.DataFrame()
        self.opn = pd.DataFrame()
        self.high = pd.DataFrame()
        self.low = pd.DataFrame()
        self.volume = pd.DataFrame()
        self.oi = pd.DataFrame()

        for c in self.cmd_list:
            queryArgs = {'wind_code': c, 'date': {'$gte': datetime.strptime(start_date, '%Y%m%d'),
                                                  '$lte': datetime.strptime(end_date, '%Y%m%d')}}
            projectionField = ['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
            res = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            fut_df = pd.DataFrame.from_records(res, index='date')
            fut_df.drop(columns=['_id'], inplace=True)
            self.cls = self.cls.join(fut_df[['CLOSE']], how='outer')
            self.cls.rename(columns={'CLOSE': c}, inplace=True)
            self.opn = self.opn.join(fut_df[['OPEN']], how='outer')
            self.opn.rename(columns={'OPEN': c}, inplace=True)
            self.high = self.high.join(fut_df[['HIGH']], how='outer')
            self.high.rename(columns={'HIGH': c}, inplace=True)
            self.low = self.low.join(fut_df[['LOW']], how='outer')
            self.low.rename(columns={'LOW': c}, inplace=True)
            self.volume = self.volume.join(fut_df[['VOLUME']], how='outer')
            self.volume.rename(columns={'VOLUME': c}, inplace=True)
            self.oi = self.oi.join(fut_df[['OI']], how='outer')
            self.oi.rename(columns={'OI': c}, inplace=True)

        self.rtn = self.cls.pct_change()

    # 排序
    def rank(self, df):
        return df.rank(axis=1)

    # period前的数据
    def delay(self, df, period):
        return df.shift(periods=period)

    # 计算两个dataframe的滚动相关性
    def correlation(self, df1, df2, period, **kwargs):
        return df1.rolling(window=period, **kwargs).corr(df2)

    # 计算两个dataframe的滚动协方差
    def covariance(self, df1, df2, period, **kwargs):
        return df1.rolling(window=period, **kwargs).cov(df2)

    # 进行等比例的放大或缩小
    def scale(self, df, scale_value=1):
        sum_df = df.sum(axis=1)
        res = pd.DataFrame()
        for c in df:
            res[c] = df[c] / sum_df * scale_value
        return res

    # 与前几天的差值
    def delta(self, df, period):
        return df - self.delay(df, period)

    # 线性衰减权重加权的均值
    def decay_linear(self, df, period, **kwargs):
        def weighted_ma(x):
            w = np.arange(1, len(x) + 1)
            return np.nansum(x * w) / np.sum(w[~np.isnan(x)])
        return df.rolling(window=period, **kwargs).apply(func=weighted_ma, raw=True)

    # 过去一段时间的最小值
    def ts_min(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).min()

    # 过去一段时间的最大值
    def ts_max(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).max()

    # 过去一段时间最小值距离当前日期的天数
    def ts_argmin(self, df, period, **kwargs):
        def argmin_days(x):
            return len(x) - np.nanargmin(x) - 1
        return df.rolling(window=period, **kwargs).apply(func=argmin_days, raw=True)

    # 过去一段时间最大值距离当前日期的天数
    def ts_argmax(self, df, period, **kwargs):
        def argmax_days(x):
            return len(x) - np.nanargmax(x) - 1
        return df.rolling(window=period, **kwargs).apply(func=argmax_days, raw=True)

    # 当前值在过去一段时间的排序
    def ts_rank(self, df, period, **kwargs):
        def tsrank(x):
            return np.argsort(np.argsort(x))[-1] + 1 if ~np.isnan(x[-1]) else np.nan
        return df.rolling(window=period, **kwargs).apply(func=tsrank, raw=True)

    # 过去一段时间的总和
    def ts_sum(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).sum()

    # 过去一段时间的乘积
    def ts_prod(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).apply(func=np.nanprod, raw=True)

    # 过去一段时间的波动率
    def ts_std(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).std()

    # 过去一段时间的均值
    def ts_mean(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).mean()

    # 过去一段时间的中位数
    def ts_median(self, df, period, **kwargs):
        return df.rolling(window=period, **kwargs).median()


if __name__ == '__main__':
    fg = FacGenerator(start_date='20130101', end_date='20181231')
    print(fg.ts_median(fg.volume, period=20, min_periods=15))










