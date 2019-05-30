
from lib.simulator.base import BacktestSys, HoldingClass, DataClass
import numpy as np
import re
import pandas as pd

class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        future_price = self.data['future_price']
        spot_price = self.data['spot_price']
        inventory = self.data['inventory']
        basis_spread_ratio = {}
        holdings_dict = {}
        fp_df = pd.DataFrame()
        opn_df = pd.DataFrame()
        high_df = pd.DataFrame()
        low_df = pd.DataFrame()
        vol_df = pd.DataFrame()
        sp_df = pd.DataFrame()
        iv_df = pd.DataFrame()


        for k, v in future_price.items():
            fp_df[v.commodity] = v.CLOSE
            opn_df[v.commodity] = v.OPEN
            high_df[v.commodity] = v.HIGH
            low_df[v.commodity] = v.LOW
            vol_df[v.commodity] = v.VOLUME

        for k, v in spot_price.items():
            if 'price' in v.__dict__:
                sp_df[v.commodity] = v.price
            elif 'CLOSE' in v.__dict__:
                sp_df[v.commodity] = v.CLOSE
        for k, v in inventory.items():
            if 'inventory' in v.__dict__ and v.commodity not in iv_df:
                iv_df[v.commodity] = v.inventory
            elif 'inventory' in v.__dict__ and v.commodity in iv_df:
                iv_df[v.commodity] += v.inventory
            elif 'CLOSE' in v.__dict__ and v.commodity not in iv_df:
                iv_df[v.commodity] = v.CLOSE
            elif 'CLOSE' in v.__dict__ and v.commodity in iv_df:
                iv_df[v.commodity] += v.CLOSE

        fp_df.index = self.dt
        sp_df.index = self.dt
        iv_df.index = self.dt
        opn_df.index = self.dt
        high_df.index = self.dt
        low_df.index = self.dt
        vol_df.index = self.dt

        rtn_df = fp_df.pct_change(periods=30)
        rtn1_df = fp_df.pct_change()

        def tsrank(x):
            return np.argsort(np.argsort(x))[-1] + 1 if ~np.isnan(x[-1]) else np.nan
        tsrank_rtn = rtn1_df.rolling(window=20, min_periods=15).apply(func=tsrank, raw=True)


        # 库存变化率
        iv_change = iv_df.pct_change(periods=5)

        # close与volume的相关性
        corr_cls_vol = pd.DataFrame(fp_df).rolling(window=20, min_periods=15).corr(vol_df)

        # 现货价格向后移一位
        sp_df = sp_df.shift(periods=1)
        bs_df = 1. - fp_df / sp_df

        bs_rank = bs_df.rank(axis=1)
        bs_rank_count = bs_rank.count(axis=1)

        iv_rank = iv_change.rank(axis=1)
        iv_rank_count = iv_rank.count(axis=1)

        bs_iv = bs_rank - iv_rank
        bs_iv_rank = bs_iv.rank(axis=1)
        bs_iv_count = bs_iv_rank.count(axis=1)

        rtn_rank = rtn_df.rank(axis=1)
        rtn_rank_count = rtn_rank.count(axis=1)

        iv_rtn = iv_rank / rtn_rank
        iv_rtn_rank = iv_rtn.rank(axis=1)
        iv_rtn_count = iv_rtn_rank.count(axis=1)

        corr_rank = tsrank_rtn.rank(axis=1)
        corr_count = tsrank_rtn.count(axis=1)

        # holdings_num = np.minimum(bs_iv_count // 2, 3)
        holdings_iv_num = np.minimum(iv_rank_count // 2, 3)
        holdings_bs_num = np.minimum(bs_rank_count // 2, 3)
        holdings_rtn_num = np.minimum(iv_rank_count // 2, 3)
        holdings_corr_num = np.minimum(corr_count // 2, 3)

        holdings_iv_num[holdings_iv_num == 0] = np.nan
        holdings_bs_num[holdings_bs_num == 0] = np.nan
        holdings_rtn_num[holdings_rtn_num == 0] = np.nan
        holdings_corr_num[holdings_corr_num == 0] = np.nan
        holdings_df = pd.DataFrame(0, index=self.dt, columns=bs_rank.columns)

        for c in holdings_df:
            # holdings_df[c][bs_iv_rank[c] > bs_iv_count - holdings_num] = 1
            # holdings_df[c][bs_iv_rank[c] <= holdings_num] = -1
            holdings_df[c][iv_rank[c] > iv_rank_count - holdings_iv_num] += -1
            holdings_df[c][iv_rank[c] <= holdings_iv_num] += 1
            holdings_df[c][bs_rank[c] > bs_rank_count - holdings_bs_num] += 1
            holdings_df[c][bs_rank[c] <= holdings_bs_num] += -1
            holdings_df[c][rtn_rank[c] > rtn_rank_count - holdings_rtn_num] += 1
            holdings_df[c][rtn_rank[c] <= holdings_rtn_num] += -1
            # holdings_df[c][corr_rank[c] > corr_count - holdings_corr_num] += 1
            # holdings_df[c][corr_rank[c] <= holdings_corr_num] += -1

        holdings = HoldingClass(self.dt)

        for k, v in self.data['future_price'].items():
            for c in holdings_df:
                if v.commodity == c:
                    holdings.add_holdings(k, holdings_df[c].values.flatten())

        return holdings


if __name__ == '__main__':
    a = BasisSpread()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=1)
    # for h in holdings.asset:
    #     holdings.update_holdings(h, 5 * getattr(holdings, h))
    holdings = a.holdingsProcess(holdings)

    a.displayResult(holdings, saveLocal=True)


