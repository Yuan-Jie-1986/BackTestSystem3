
import yaml
import os
import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pprint
from base import BacktestSys


class test(BacktestSys):
    def __init__(self):
        self.current_file = __file__
        self.prepare()

    def strategy(self):
        wgts_dict = {}
        for k, v in self.category.items():
            cls = self.data[k]['CLOSE']
            ma20 = pd.DataFrame(cls).rolling(window=20).mean().values.flatten()
            ma10 = pd.DataFrame(cls).rolling(window=10).mean().values.flatten()
            con = np.zeros(cls.shape)
            con[(cls > ma20) * (cls > ma10)] = 1
            con[(cls < ma10) * (cls > ma20)] = 0
            con[(cls < ma20) * (cls < ma10)] = -1
            con[(cls > ma10) * (cls < ma20)] = 0
            wgts_dict[k] = con
        return wgts_dict


if __name__ == '__main__':

    a = test()
    wgts = a.strategy()
    wgts = a.wgtsStandardization(wgts)
    wgts = a.wgtsProcess(wgts)
    a.displayResult(wgts)
