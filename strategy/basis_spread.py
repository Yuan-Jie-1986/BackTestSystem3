
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
        sp_df = pd.DataFrame()
        iv_df = pd.DataFrame()

        for k, v in future_price.items():
            fp_df[v.commodity] = v.CLOSE
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

        print(iv_df)

        # 现货价格向后移一位
        sp_df = sp_df.shift(periods=1)
        bs_df = 1. - fp_df / sp_df

        bs_rank = bs_df.rank(axis=1)
        bs_rank_count = bs_rank.count(axis=1)
        holdings_num = np.minimum(bs_rank_count // 2, 3)
        holdings_df = pd.DataFrame(0, index=self.dt, columns=bs_rank.columns)

        for c in holdings_df:
            holdings_df[c][bs_rank[c] > bs_rank_count - holdings_num] = 1
            holdings_df[c][bs_rank[c] <= holdings_num] = -1

        print(holdings_df)


        # for k1, v1 in future_price.items():
        #     holdings_dict[k1] = np.zeros(len(self.dt))
        #     fp = v1.CLOSE
        #     for k2, v2 in spot_price.items():
        #         if v2.commodity == v1.commodity:
        #             if 'price' in v2.__dict__:
        #                 sp = v2.price
        #             elif 'CLOSE' in v2.__dict__:
        #                 sp = v2.CLOSE
        #     sp_new = np.ones_like(sp) * np.nan
        #     sp_new[1:] = sp[1:]
        #     basis_spread_ratio[k1] = 1 - fp / sp_new


        holdings = HoldingClass(self.dt)

        for k, v in self.data['future_price'].items():
            for c in holdings_df:
                if v.commodity == c:
                    print(holdings_df[c].values.flatten())
                    holdings.add_holdings(k, holdings_df[c].values.flatten())

        # for i in np.arange(len(self.dt)):
        #
        #     # 根据基差比例进行交易，多正基差最大的n只，空负基差最小的n只
        #     bsr_daily = []
        #     for k in basis_spread_ratio:
        #         bsr_daily.append(basis_spread_ratio[k][i])
        #     bsr_daily = np.array(bsr_daily)
        #     count = len(bsr_daily[~np.isnan(bsr_daily)])
        #     if count <= 1:
        #         continue
        #     bsr_series = bsr_daily[~np.isnan(bsr_daily)]
        #     bsr_series.sort()
        #     num_selection = min(3, count // 2)
        #     low_point = bsr_series[num_selection-1]
        #     high_point = bsr_series[-num_selection]
        #
        #     for k in basis_spread_ratio:
        #         if basis_spread_ratio[k][i] <= low_point:
        #             holdings_dict[k][i] = -1  #- int(5. * wgt_daily[k])
        #         elif basis_spread_ratio[k][i] >= high_point:
        #             holdings_dict[k][i] = 1  # int(5. * wgt_daily[k])
        #


        # for k, v in holdings_dict.items():
        #     holdings.add_holdings(k, v)

        return holdings


if __name__ == '__main__':
    a = BasisSpread()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=1)
    # for h in holdings.asset:
    #     holdings.update_holdings(h, 2 * getattr(holdings, h))
    holdings = a.holdingsProcess(holdings)

    a.displayResult(holdings, saveLocal=True)


