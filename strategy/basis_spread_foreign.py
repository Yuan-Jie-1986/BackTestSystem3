
from lib.simulator.base import BacktestSys
import numpy as np
import pandas as pd
import re

class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        # 生成一个字典，对于每个品种的，期货和现货的名字
        pairs_dict = {}
        for k, v in list(self.category.items()):
            if v not in pairs_dict:
                pairs_dict[v] = [k]
            else:
                pairs_dict[v].append(k)

        basis_spread_ratio = {}
        wgtsDict = {}
        patten1 = re.compile('c1\Z')  # 判断是否为近一合约
        patten2 = re.compile('c2\Z')  # 判断是否为近二合约
        for k, v in pairs_dict.items():
            for sub_v in v:
                wgtsDict[sub_v] = np.zeros_like(self.dt)
                if patten1.search(sub_v):
                    c1_price = self.data[sub_v]['CLOSE']
                elif patten2.search(sub_v):
                    c2_price = self.data[sub_v]['CLOSE']

            # basis_spread_ratio[k] = 1 - c2_price / c1_price
            bs = 1 - c2_price / c1_price
            # bs = c1_price - c2_price
            bs_mean = pd.DataFrame(bs).rolling(window=21, min_periods=15).mean().values.flatten()
            bs_std = pd.DataFrame(bs).rolling(window=21, min_periods=15).std().values.flatten()
            basis_spread_ratio[k] = (bs - bs_mean) / bs_std



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
            num_selection = min(3, count / 2)
            low_point = bsr_series[num_selection-1]
            high_point = bsr_series[-num_selection]

            # # 计算得到各合约的20日波动
            # vol_daily = {}
            # wgt_daily = {}
            # n = max(0, i - 240)
            # for k in basis_spread_ratio:
            #     # vol_daily[k] = np.std(self.data[k]['CLOSE'][n:i]) * self.unit[self.category[k]]
            #     vol_daily[k] = self.data[k]['CLOSE'][i] * self.unit[self.category[k]]
            #     wgt_daily[k] = 1. / vol_daily[k]
            #     # if np.isinf(wgt_daily[k]):
            #     #     print wgt_daily[k], k, vol_daily[k]
            #
            # wgt_min = np.nanmin(wgt_daily.values())
            # for k in wgt_daily:
            #     wgt_daily[k] = wgt_daily[k] / wgt_min
            #     if ~np.isfinite(wgt_daily[k]):
            #         wgt_daily[k] = 0.
            #     # if np.isinf(wgt_daily[k]):
            #     #     wgt_daily[k] = 1.


            for k in basis_spread_ratio:
                if basis_spread_ratio[k][i] <= low_point:
                    for pa in pairs_dict[k]:
                        if patten1.search(pa):
                            wgtsDict[pa][i] = 10
                        if patten2.search(pa):
                            wgtsDict[pa][i] = -10

                elif basis_spread_ratio[k][i] >= high_point:
                    for pa in pairs_dict[k]:
                        if patten1.search(pa):
                            wgtsDict[pa][i] = -10
                        if patten2.search(pa):
                            wgtsDict[pa][i] = 10
        # for k in wgtsDict:
        #     wgt = np.zeros_like(wgtsDict[k])
        #     wgt[1:] = wgtsDict[k][:-1]
        #     wgtsDict[k] = wgt

        print(wgtsDict)
        return wgtsDict


if __name__ == '__main__':
    a = BasisSpread()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict, mode=1)
    wgtsDict = a.wgtsProcess(wgtsDict)
    for k in wgtsDict:
        wgtsDict[k] = 8 * np.array(wgtsDict[k])
    a.displayResult(wgtsDict, saveLocal=True)


