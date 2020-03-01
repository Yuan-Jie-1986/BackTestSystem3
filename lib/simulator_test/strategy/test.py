
import yaml
import os
import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pprint
from BackTestSystem3.lib.simulator_test.simlib.hObject import HoldingClass
from BackTestSystem3.lib.simulator_test.simlib.processor import BacktestSys



class test(BacktestSys):
    def __init__(self):
        self.current_file = __file__
        self.prepare()

    def strategy(self):
        holdingsObj = HoldingClass(self.dt)
        ta = self.data['bt_price']['TA_MIN']
        ta_cls = ta.close
        # print(ta.__dict__)
        # print(self.dt)
        # print(ta_cls)
        # px = self.data['bt_price']['PX']
        # px_cls = px.price
        # dollar = self.dollar2rmb.CLOSE
        # profit = ta_cls - (px_cls * 1.02 * 1.17 * 0.656 * dollar)
        print(ta_cls)
        print(self.dt)

        ma20 = pd.DataFrame(ta_cls).rolling(window=20).mean().values.flatten()
        ma10 = pd.DataFrame(ta_cls).rolling(window=10).mean().values.flatten()

        con = np.zeros(self.dt.shape)
        con[(ta_cls > ma20) & (ta_cls > ma10)] = 1
        con[(ta_cls < ma10) & (ta_cls > ma20)] = 0
        con[(ta_cls < ma20) & (ta_cls < ma10)] = -1
        con[(ta_cls > ma10) & (ta_cls < ma20)] = 0
        holdingsObj.add_holdings('TA_DAILY', con)
        # holdingsObj.add_holdings('PX', -con)



        # for k, v in self.data['future_price'].items():
        #     cls = v.CLOSE
        #     ma20 = pd.DataFrame(cls).rolling(window=20).mean().values.flatten()
        #     ma10 = pd.DataFrame(cls).rolling(window=10).mean().values.flatten()
        #     con = np.zeros(cls.shape)
        #     con[(cls > ma20) * (cls > ma10)] = 1
        #     con[(cls < ma10) * (cls > ma20)] = 0
        #     con[(cls < ma20) * (cls < ma10)] = -1
        #     con[(cls > ma10) * (cls < ma20)] = 0
        #     holdingsObj.add_holdings(k, con)
        return holdingsObj


if __name__ == '__main__':

    a = test()
    holdings = a.strategy()
    holdings = a.holdingsStandardization(holdings, mode=6)
    holdings = a.holdingsProcess(holdings)
    a.displayResult(holdings)
