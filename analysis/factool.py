
import numpy as np
import pandas as pd

class FactorAnalysis(object):
    def __init__(self, fac, cls):
        # fac和cls是dataframe的形式，index是日期
        self.factor = fac
        self.fac_cls = cls

    def normal_ic(self, ic_len=20, ir_len=60):
        rtn = self.fac_cls.pct_change(periods=ic_len, fill_method='ffill')
        rtn = rtn.shift(periods=-ic_len)
        ic = self.factor.corrwith(rtn, axis=1, drop=True)
        ic = pd.DataFrame({'NormalIC': ic})
        ic_mean = ic.rolling(window=ir_len).mean()
        ic_std = ic.rolling(window=ir_len).std()
        ic['NormalIR'] = ic_mean / ic_std
        return ic

    def rank_ic(self, ic_len=20, ir_len=60):
        rtn = self.fac_cls.pct_change(periods=ic_len, fill_method='ffill')
        rtn = rtn.shift(periods=-ic_len)
        fac_rank = self.factor.rank(axis=1, na_option='keep')
        rtn_rank = rtn.rank(axis=1, na_option='keep')
        ic = fac_rank.corrwith(rtn_rank, axis=1, drop=True)
        ic = pd.DataFrame({'RankIC': ic})
        ic_mean = ic.rolling(window=ir_len).mean()
        ic_std = ic.rolling(window=ir_len).std()
        ic['RankIR'] = ic_mean / ic_std
        return ic

    def fac_rtn(self, fac_num=2, rtn_len=20, ir_len=60):
        # 如果有效数据不足，计算得到nan值
        rtn = self.fac_cls.pct_change(periods=rtn_len, fill_method='ffill')
        rtn = rtn.shift(periods=-rtn_len)

        # 如果有效数据小于2倍的fac_num，则将rtn赋为nan
        fac_rank_nan_num = self.factor.count(axis=1)
        fac_rank_nan_flag = fac_rank_nan_num < 2 * fac_num
        rtn[fac_rank_nan_flag] = np.nan

        high_flag = pd.DataFrame()
        fac_rank_4_high = self.factor.rank(axis=1, method='max', na_option='keep')
        for c in fac_rank_4_high:
            high_flag[c] = fac_rank_4_high[c] <= fac_rank_nan_num - fac_num

        low_flag = pd.DataFrame()
        fac_rank_4_low = self.factor.rank(axis=1, method='min', na_option='keep')
        for c in fac_rank_4_low:
            low_flag[c] = fac_rank_4_low[c] > fac_num

        rtn_high = rtn.copy()
        rtn_high[high_flag] = np.nan

        rtn_low = rtn.copy()
        rtn_low[low_flag] = np.nan

        res = rtn_high.mean(axis=1) - rtn_low.mean(axis=1)

        res = pd.DataFrame({'FacRtn': res})
        res_mean = res.rolling(window=ir_len).mean()
        res_std = res.rolling(window=ir_len).std()
        res['RtnIR'] = res_mean / res_std

        return res

    def effective_num(self):
        return pd.DataFrame(self.fac_cls.count(axis=1), columns=['number'])






if __name__ == '__main__':
    a = pd.DataFrame(np.random.randn(20, 10))
    b = pd.DataFrame(np.random.randn(20, 10))
    a.iloc[:, 5:9] = np.nan
    # b.iloc[:, 2:9] = np.nan
    print(a, b)
    d = FactorAnalysis(a, b)
    print(d.fac_rtn(fac_num=3, rtn_len=3))
    print(d.fac_rtn(fac_num=2, rtn_len=3))
    print(d.fac_rtn(fac_num=4, rtn_len=3))
    # print d.effective_num()




