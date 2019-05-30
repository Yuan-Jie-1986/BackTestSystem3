# coding=utf-8
from lib.simulator.base import BacktestSys, HoldingClass
import numpy as np
import pandas as pd
from datetime import datetime
import re

class Deviation(BacktestSys):
    def __init__(self):
        super(Deviation, self).__init__()

    def strategy(self):

        formulas = [('VAR1 - VAR2', ('L.DCE', 'PP.DCE')),
                    ('VAR1 - VAR2', ('L.DCE', 'TA.CZC')),
                    ('VAR1 - 1.2 * VAR2 - 50', ('J.DCE', 'JM.DCE')),
                    ('VAR1 - VAR2', ('L.DCE', 'V.DCE')),
                    ('1./3. * VAR3 - 2 * VAR2 + 1.85 * VAR1 + 637', ('ZC.CZC', 'MA.CZC', 'PP.DCE')),
                    ('VAR1 - 3 * VAR2', ('PP.DCE', 'MA.CZC')),
                    ('VAR1 - 1.85 * VAR2 - 637', ('MA.CZC', 'ZC.CZC')),
                    ('VAR1 - VAR2', ('PP.DCE', 'V.DCE')),
                    ('VAR1 - VAR2', ('TA.CZC', 'RU.SHF')),
                    ('VAR1 - VAR2', ('TA.CZC', 'BU.SHF')),
                    ('VAR1 - 2 * VAR2', ('V.DCE', 'J.DCE')),
                    ('VAR1 - 1.7 * VAR3 - 0.5 * VAR2 - 800', ('RB.SHF', 'J.DCE', 'I.DCE')),
                    ('VAR1 - VAR2', ('HC.SHF', 'RB.SHF')),
                    ('VAR1 - 0.95 * VAR2 - 1000', ('HC.SHF', 'J.DCE')),
                    ('VAR1 - 3.5 * VAR2 - 800', ('RB.SHF', 'I.DCE')),
                    ('VAR1 - VAR2', ('J.DCE', 'ZC.CZC')),]
                    # ('VAR1 - VAR2', ('BU.SHF', 'FU.SHF'))]

        holdings = HoldingClass(self.dt)
        wgtsDict= {}
        deviation_dict = {}

        future_price = self.data['future_price']
        for f, v in formulas:
            wgts_formulas = {}
            ptn = re.compile('VAR\d+')
            res = ptn.findall(f)
            if len(res) != len(v):
                raise Exception(u'公式提供错误')
            cls_df = pd.DataFrame(index=self.dt)
            for i in np.arange(len(v)):
                if v[i] not in deviation_dict:
                    deviation_dict[v[i]] = np.zeros_like(self.dt)
                if v[i] not in wgtsDict:
                    wgtsDict[v[i]] = np.zeros_like(self.dt)
                if v[i] not in wgts_formulas:
                    wgts_formulas[v[i]] = np.zeros_like(self.dt)
                cls_df[v[i]] = future_price[v[i]].CLOSE
                f = f.replace(res[i], 'cls_df["%s"]' % v[i])
            cls_df['price_diff'] = eval(f)
            cls_df['price_diff_ma'] = cls_df[['price_diff']].rolling(window=60, min_periods=50).mean()
            cls_df['price_diff_std'] = cls_df[['price_diff']].rolling(window=60, min_periods=50).std()
            # cls_df['rtn_standard'] = (cls_df['price_diff'] - cls_df['price_diff_ma']) / cls_df['price_diff_std']
            cls_df['rtn'] = cls_df['price_diff'] - cls_df['price_diff_ma']
            cls_df['rtn_std'] = cls_df['rtn'].rolling(window=20).std()
            cls_df['rtn_mean'] = cls_df['rtn'].rolling(window=20).mean()
            cls_df['rtn_standard'] = (cls_df['rtn'] - cls_df['rtn_mean']) / cls_df['rtn_std']
            rtn_standard = cls_df['rtn_standard'].values.flatten()

            print(f)
            for j in np.arange(len(v)):
                ptn_str = '[+-](?=[0-9\. \*/]*?cls_df\["%s"\])' % v[j]
                ptn = re.compile(ptn_str)
                if ptn.search(f):
                    sign_v = ptn.search(f).group()
                else:
                    sign_v = '+'
                print(v[j], sign_v)
                deviation_dict[v[j]] += rtn_standard * int(sign_v + '1')

                # for i in np.arange(1, len(self.dt)):
                #     if rtn_standard[i] >= 3. and wgts_formulas[v[j]][i-1] == 0:
                #         wgts_formulas[v[j]][i] = -1 * int(sign_v + '1')
                #     elif rtn_standard[i] <= -3. and wgts_formulas[v[j]][i-1] == 0:
                #         wgts_formulas[v[j]][i] = 1 * int(sign_v + '1')
                #     elif rtn_standard[i] < 0 and wgts_formulas[v[j]][i-1] == -1 * int(sign_v + '1'):
                #         wgts_formulas[v[j]][i] = 0.
                #     elif rtn_standard[i] > 0 and wgts_formulas[v[j]][i-1] == 1 * int(sign_v + '1'):
                #         wgts_formulas[v[j]][i] = 0.
                #     else:
                #         wgts_formulas[v[j]][i] = wgts_formulas[v[j]][i-1]
                # print(wgts_formulas)
                #
                # wgtsDict[v[j]] = wgtsDict[v[j]] + wgts_formulas[v[j]]

        deviation_df = pd.DataFrame.from_dict(deviation_dict)
        deviation_df.index = self.dt

        deviation_rank = deviation_df.rank(axis=1)
        deviation_count = deviation_df.count(axis=1)

        holdings_num = np.minimum(deviation_count // 2, 3)
        holdings_num[holdings_num == 0] = np.nan

        holdings_df = pd.DataFrame(0, index=self.dt, columns=deviation_rank.columns)

        for c in holdings_df:
            holdings_df[c][deviation_rank[c] > deviation_count - holdings_num] = 1
            holdings_df[c][deviation_rank[c] <= holdings_num] = -1

        for c in holdings_df:
            holdings.add_holdings(c, holdings_df[c].values.flatten())

        return holdings


if __name__ == '__main__':
    a = Deviation()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=1)
    holdings = a.holdingsProcess(holdings)

    a.displayResult(holdings)

