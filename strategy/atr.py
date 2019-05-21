# coding=utf-8

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

        period = 30

        wgtsDict = {}
        for k in self.data:
            cls = self.data[k]['CLOSE']
            high = self.data[k]['HIGH']
            low = self.data[k]['LOW']
            vol = self.data[k]['VOLUME']
            oi = self.data[k]['OI']

            wgtsDict[k] = np.zeros(len(self.dt))

            p1 = high - low
            p2 = np.ones(len(self.dt)) * np.nan
            p2[1:] = np.abs(high[1:] - cls[:-1])
            p3 = np.ones(len(self.dt)) * np.nan
            p3[1:] = np.abs(cls[:-1] - low[1:])

            true_range = np.maximum(p1, np.maximum(p2, p3))
            atr = pd.DataFrame(true_range).rolling(window=period).mean().values.flatten()
            ma20 = pd.DataFrame(cls).rolling(window=period).mean().values.flatten()
            upper = ma20 + atr
            lower = ma20 - atr
            volma = pd.DataFrame(vol).rolling(window=period).mean().values.flatten()


            for i in np.arange(1, len(self.dt)):
                if wgtsDict[k][i-1] == 0 and cls[i] > upper[i] and cls[i-1] <= upper[i-1]: # and vol[i] < volma[i]:
                    wgtsDict[k][i] = 1
                elif wgtsDict[k][i-1] == 0 and cls[i] < lower[i] and cls[i-1] >= lower[i-1]: # and vol[i] < volma[i]:
                    wgtsDict[k][i] = -1
                elif wgtsDict[k][i-1] == 1 and cls[i] < ma20[i] and cls[i-1] >= ma20[i-1]:
                    wgtsDict[k][i] = 0
                elif wgtsDict[k][i-1] == -1 and cls[i] > ma20[i] and cls[i-1] <= ma20[i-1]:
                    wgtsDict[k][i] = 0
                else:
                    wgtsDict[k][i] = wgtsDict[k][i-1]
            #
            #
            # con1 = cls > upper
            # con2 = (cls < ma20) * (cls >= lower)
            # con3 = cls < lower
            # con4 = (cls >= rtn5[k]) * (rtn5[k] <= rtn20[k])
            #
            # wgtsDict[k][con1] = 1
            # wgtsDict[k][con2] = 0
            # wgtsDict[k][con3] = -1
            # wgtsDict[k][con4] = 0


        return wgtsDict


if __name__ == '__main__':
    a = BasisSpread()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict, mode=3)
    wgtsDict = a.wgtsProcess(wgtsDict)
    # for k in wgtsDict:
    #     wgtsDict[k] = 2 * np.array(wgtsDict[k])

    a.displayResult(wgtsDict, saveLocal=True)


