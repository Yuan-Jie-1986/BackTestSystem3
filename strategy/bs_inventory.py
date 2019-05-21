
from lib.simulator.base import BacktestSys
import numpy as np
import re
import pandas as pd

class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        inventory = {}
        for d in self.data:
            if self.data[d]['category'] == 'inventory':
                inventory[d] = self.data[d]['inventory']
                print(inventory[d])


        # # 生成一个字典，对于每个品种的，期货和现货的名字
        # pairs_dict = {}
        # for k, v in self.category.items():
        #     if v not in pairs_dict:
        #         pairs_dict[v] = [k]
        #     else:
        #         pairs_dict[v].append(k)
        #
        # basis_spread = {}
        # basis_spread_ratio = {}
        # wgtsDict = {}
        # patten = re.compile('(?<=\w)\.(?=[A-Z]+)')  # 用来判断是否为期货
        # for k, v in pairs_dict.items():
        #     for sub_v in v:
        #         if patten.search(sub_v):
        #             futures_contract = sub_v
        #             futures_price = self.data[sub_v]['CLOSE']
        #         else:
        #             spot_price = list(self.data[sub_v].values())[0]
        #             spot_price_new = np.ones_like(spot_price) * np.nan
        #             spot_price_new[1:] = spot_price[1:]
        #
        #     if "futures_contract" not in locals():
        #         raise Exception(u'没有期货合约')
        #
        #     # bs = 1 - c2_price / c1_price
        #     bs = 1 - futures_price / spot_price_new
        #     bs_mean = pd.DataFrame(bs).rolling(window=252, min_periods=240).mean().values.flatten()
        #     bs_std = pd.DataFrame(bs).rolling(window=252, min_periods=240).std().values.flatten()
        #     basis_spread_ratio[futures_contract] = bs #/ bs_std
        #
        #     # basis_spread[futures_contract] = spot_price_new - futures_price
        #     # basis_spread_ratio[futures_contract] = 1 - futures_price / spot_price_new
        #     wgtsDict[futures_contract] = np.zeros_like(self.dt)
        #
        #     del futures_contract
        #
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
        #     # # 计算得到各合约的20日波动
        #     # vol_daily = {}
        #     # wgt_daily = {}
        #     # n = max(0, i - 240)
        #     # for k in basis_spread_ratio:
        #     #     # vol_daily[k] = np.std(self.data[k]['CLOSE'][n:i]) * self.unit[self.category[k]]
        #     #     vol_daily[k] = self.data[k]['CLOSE'][i] * self.unit[self.category[k]]
        #     #     wgt_daily[k] = 1. / vol_daily[k]
        #     #     # if np.isinf(wgt_daily[k]):
        #     #     #     print wgt_daily[k], k, vol_daily[k]
        #     #
        #     # wgt_min = np.nanmin(wgt_daily.values())
        #     # for k in wgt_daily:
        #     #     wgt_daily[k] = wgt_daily[k] / wgt_min
        #     #     if ~np.isfinite(wgt_daily[k]):
        #     #         wgt_daily[k] = 0.
        #     #     # if np.isinf(wgt_daily[k]):
        #     #     #     wgt_daily[k] = 1.
        #
        #
        #     for k in basis_spread_ratio:
        #         if basis_spread_ratio[k][i] <= low_point:
        #             wgtsDict[k][i] = -1  #- int(5. * wgt_daily[k])
        #         elif basis_spread_ratio[k][i] >= high_point:
        #             wgtsDict[k][i] = 1  # int(5. * wgt_daily[k])

        return wgtsDict


if __name__ == '__main__':
    a = BasisSpread()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict, mode=2)
    wgtsDict = a.wgtsProcess(wgtsDict)
    for k in wgtsDict:
        wgtsDict[k] = 2 * np.array(wgtsDict[k])

    a.displayResult(wgtsDict, saveLocal=True)


