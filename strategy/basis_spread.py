
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
        print(inventory)
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

            if 'inventory' in v.__dict__:
                iv_df[v.commodity] = v.inventory
            elif 'CLOSE' in v.__dict__:
                iv_df[v.commodity] = v.CLOSE
        print(iv_df)
        iv_df.to_clipboard()


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
        for i in np.arange(len(self.dt)):

            # 根据基差比例进行交易，多正基差最大的n只，空负基差最小的n只
            bsr_daily = []
            for k in basis_spread_ratio:
                bsr_daily.append(basis_spread_ratio[k][i])
            bsr_daily = np.array(bsr_daily)
            count = len(bsr_daily[~np.isnan(bsr_daily)])
            if count <= 1:
                continue
            bsr_series = bsr_daily[~np.isnan(bsr_daily)]
            bsr_series.sort()
            num_selection = min(3, count // 2)
            low_point = bsr_series[num_selection-1]
            high_point = bsr_series[-num_selection]

            for k in basis_spread_ratio:
                if basis_spread_ratio[k][i] <= low_point:
                    holdings_dict[k][i] = -1  #- int(5. * wgt_daily[k])
                elif basis_spread_ratio[k][i] >= high_point:
                    holdings_dict[k][i] = 1  # int(5. * wgt_daily[k])



        for k, v in holdings_dict.items():
            holdings.add_holdings(k, v)
        return holdings


if __name__ == '__main__':
    a = BasisSpread()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=0)
    for h in holdings.asset:
        holdings.update_holdings(h, 2 * getattr(holdings, h))
    holdings = a.holdingsProcess(holdings)

    a.displayResult(holdings, saveLocal=True)


