
from BackTestSystem3.lib.simulator_test.base import BacktestSys, HoldingClass, DataClass
import numpy as np
import re
import pandas as pd
from datetime import datetime

class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        future_price = self.data['bt_price']
        spot_price = self.data['spot_price']
        profit_rate = self.data['profit_rate']
        inventory = self.data['inventory']
        future_index = self.data['future_index']
        df_profit = pd.DataFrame(index=self.dt)
        for k, v in profit_rate.items():
            df_profit[v.commodity] = v.upper_profit_rate

        # ========================回测的时候需要======================
        # PVC/RU/TA/BU的利润率需要往后移一天
        df_profit[['RU']] = df_profit[['RU']].shift(periods=1)

        df_profit.fillna(method='ffill', inplace=True)

        profit_slow = df_profit.rolling(window=10).mean()

        profit_mean = df_profit.rolling(window=250).mean()
        profit_std = df_profit.rolling(window=250).std()
        df_profit = (profit_slow - profit_mean) / profit_std
        # df_profit = df_profit.rolling(window=60, min_periods=50).mean()
        # profit_chg = df_profit.pct_change(periods=20)

        iv_df = pd.DataFrame(index=self.dt)
        sp_df = pd.DataFrame(index=self.dt)
        fp_df = pd.DataFrame(index=self.dt)
        index_vol = pd.DataFrame(index=self.dt)
        index_oi = pd.DataFrame(index=self.dt)

        for k, v in inventory.items():
            if 'inventory' in v.__dict__ and v.commodity not in iv_df:
                iv_df[v.commodity] = v.inventory
            elif 'inventory' in v.__dict__ and v.commodity in iv_df:
                iv_df[v.commodity] += v.inventory
            elif 'CLOSE' in v.__dict__ and v.commodity not in iv_df:
                iv_df[v.commodity] = v.CLOSE
            elif 'CLOSE' in v.__dict__ and v.commodity in iv_df:
                iv_df[v.commodity] += v.CLOSE

        # =============回测的时候需要=================
        # L和PP的库存需要往后移一周
        # iv_df[['L', 'PP']] = iv_df[['L', 'PP']].shift(periods=5)

        for k, v in spot_price.items():
            if 'price' in v.__dict__:
                sp_df[v.commodity] = v.price
            elif 'CLOSE' in v.__dict__:
                sp_df[v.commodity] = v.CLOSE

        sp_df.fillna(method='ffill', inplace=True)

        for k, v in future_price.items():
            fp_df[v.commodity] = v.CLOSE

        for k, v in future_index.items():
            index_vol[v.commodity] = v.VOLUME
            index_oi[v.commodity] = v.OI

        oi_short_mean = index_oi.rolling(window=5).mean()
        oi_long_mean = index_oi.rolling(window=20).mean()
        oi_chg = oi_short_mean / oi_long_mean

        vol_short_mean = index_vol.rolling(window=5).mean()
        vol_long_mean = index_vol.rolling(window=20).mean()
        vol_chg = vol_short_mean / vol_long_mean

        df_profit = df_profit * oi_chg * vol_chg
        df_profit.dropna(axis=1, inplace=True, how='all')

        # df_profit.to_csv('profit_value.csv')

        profit_rank = df_profit.rank(axis=1)
        profit_count = profit_rank.count(axis=1)
        # profit_rank.to_csv('pr_rank_%s.csv' % datetime.today().strftime('%Y%m%d'))

        holdings_profit_num = np.minimum(profit_count // 2, 3)
        holdings_profit_num[holdings_profit_num == 0] = np.nan

        # 库存变化率
        # iv_df = iv_df.shift(periods=1)
        iv_mean = iv_df.rolling(window=60).mean()
        iv_std = iv_df.rolling(window=60).std()
        iv_change = (iv_df - iv_mean) / iv_std

        iv_change = iv_change * oi_chg * vol_chg

        # iv_change.to_csv('iv_value.csv')

        # iv_change = iv_df.pct_change(periods=5)
        iv_rank = iv_change.rank(axis=1)
        iv_rank_count = iv_rank.count(axis=1)
        # iv_rank.to_csv('iv_rank_%s.csv' % datetime.today().strftime('%Y%m%d'))

        holdings_iv_num = np.minimum(iv_rank_count // 2, 3)
        holdings_iv_num[holdings_iv_num == 0] = np.nan

        # ================回测的时候需要========================
        # 现货价格需要往后移一天
        # sp_df = sp_df.shift(periods=1)
        # sp_df[['MA', 'EG', 'PP', 'TA', 'V', 'RU', 'BU', 'L', 'JM', 'ZC']] = sp_df[
        #     ['MA', 'EG', 'PP', 'TA', 'V', 'RU', 'BU', 'L', 'JM', 'ZC']].shift(periods=1)
        sp_df['I'] = sp_df['I'] / 0.92

        sp_df = sp_df.rolling(window=60).mean()
        fp_df = fp_df.rolling(window=60).mean()

        bs_df = 1. - fp_df[sp_df.columns] / sp_df
        # bs_df = sp_df - fp_df[sp_df.columns]
        # bs_mean = bs_df.rolling(window=250, min_periods=200).mean()
        # bs_std = bs_df.rolling(window=250, min_periods=200).std()
        # bs_df = (bs_df - bs_mean) / bs_std

        # bs_df = bs_df * oi_chg * vol_chg

        # bs_df.to_csv('bs_value.csv')

        bs_rank = bs_df.rank(axis=1)
        bs_rank_count = bs_rank.count(axis=1)
        # bs_rank.to_csv('bs_rank_%s.csv' % datetime.today().strftime('%Y%m%d'))

        holdings_bs_num = np.minimum(bs_rank_count // 2, 3)
        holdings_bs_num[holdings_bs_num == 0] = np.nan

        rtn_df = fp_df.pct_change(periods=10)
        rtn_df = rtn_df * vol_chg * oi_chg
        rtn_rank = rtn_df.rank(axis=1)
        rtn_count = rtn_rank.count(axis=1)
        holdings_rtn_num = np.minimum(rtn_count // 2, 3)
        holdings_rtn_num[holdings_rtn_num == 0] = np.nan

        holdings_df = pd.DataFrame(0, index=self.dt, columns=list(future_price.keys()))

        for c in holdings_df:
            for k, v in future_price.items():
                if k == c:

                    holdings_df[c][iv_rank[v.commodity] > iv_rank_count - holdings_iv_num] += -1
                    holdings_df[c][iv_rank[v.commodity] <= holdings_iv_num] += 1

                    holdings_df[c][bs_rank[v.commodity] > bs_rank_count - holdings_bs_num] += 1
                    holdings_df[c][bs_rank[v.commodity] <= holdings_bs_num] += -1

                    if v.commodity not in profit_rank:
                        continue

                    holdings_df[c][profit_rank[v.commodity] > profit_count - holdings_profit_num] += -1
                    holdings_df[c][profit_rank[v.commodity] <= holdings_profit_num] += 1

                    # holdings_df[c][rtn_rank[v.commodity] > rtn_count - holdings_rtn_num] += 1
                    # holdings_df[c][rtn_rank[v.commodity] <= holdings_rtn_num] = -1

        # holdings_df.to_csv('holdings.csv')

        holdings = HoldingClass(self.dt)

        for c in holdings_df:
            holdings.add_holdings(c, holdings_df[c].values.flatten())

        return holdings


if __name__ == '__main__':
    a = BasisSpread()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=6)
    # for h in holdings.asset:
    #     holdings.update_holdings(h, 2 * getattr(holdings, h))
    holdings = a.holdingsProcess(holdings)

    a.displayResult(holdings, saveLocal=True)
    # res = a.getTotalResult(holdings, show=False)
    # print(res.loc[['total']])


