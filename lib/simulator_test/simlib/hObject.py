"""
用于回测的持仓类
auther: YUANJIE
"""

import numpy as np
import pandas as pd


# 持仓数据类


class HoldingClass(object):
    def __init__(self, ts):
        # self.asset存储所持有的合约名
        # self.合约名是持仓数据
        self.ts = ts
        self.asset = []
        self.newest_holdings = {}

    # 检查时间长度与持仓变量长度是否一致
    def check_len(self):
        ts_len = len(self.ts)
        for field in self.asset:
            temp = getattr(self, field)
            if len(temp) != ts_len:
                raise Exception('%s的持仓数据与时间的长度不一致，%s的长度是%d, 时间的长度是%d' %
                                (field, field, len(temp), ts_len))

    # 自动检查长度是否一致的装饰器
    def auto_check(func):
        def inner(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.check_len()
            return
        return inner

    # 定义持仓数据进行叠加
    def __add__(self, other):
        if (self.ts != other.ts).any():
            raise Exception('两个持仓数据的时间不一致')
        new = HoldingClass(self.ts)
        new.asset = np.array(list(set(self.asset).union(set(other.asset))))
        set_inter = set(self.asset).intersection(set(other.asset))
        for h in set_inter:
            new_h = getattr(self, h) + getattr(other, h)
            setattr(new, h, new_h)
            new.newest_holdings[h] = self.newest_holdings[h] + other.newest_holdings[h]
        set_1 = set(self.asset).difference(set(other.asset))
        for h in set_1:
            setattr(new, h, getattr(self, h))
            new.newest_holdings[h] = self.newest_holdings[h]
        set_2 = set(other.asset).difference(set(self.asset))
        for h in set_2:
            setattr(new, h, getattr(other, h))
            new.newest_holdings[h] = other.newest_holdings[h]
        return new


    # 增加持仓数据
    @auto_check
    def add_holdings(self, contract, holdings):
        self.asset.append(contract)
        holdings = np.array(holdings)
        setattr(self, contract, holdings)
        self.newest_holdings[contract] = holdings[-1]

    # 更新持仓数据
    @auto_check
    def update_holdings(self, contract, holdings):
        if contract not in self.asset:
            raise Exception('%s合约不在已有的持仓列表中，请检查或使用add_holdings函数' % contract)
        holdings = np.array(holdings)
        setattr(self, contract, holdings)
        self.newest_holdings[contract] = holdings[-1]


    # 将持仓情况向后平移
    @auto_check
    def shift_holdings(self, mode='all', label=None):
        if mode == 'all':
            # 所有持仓全部往后平移，此为默认方式
            for h in self.asset:
                temp = getattr(self, h)
                new_holdings = np.zeros(len(self.ts))
                new_holdings[1:] = temp[:-1]
                setattr(self, h, new_holdings)
        elif mode == 'single':
            if label in self.asset:
                temp = getattr(self, label)
                new_holdings = np.zeros(len(self.ts))
                new_holdings[1:] = temp[:-1]
                setattr(self, label, new_holdings)

    # 获得最新持仓数据
    def get_newest_holdings(self):
        return self.newest_holdings

    # 根据调仓周期对持仓进行调整
    @auto_check
    def adjust_holdings_turnover(self, turnover):
        holding_df = pd.DataFrame()
        for h in self.asset:
            holding_df[h] = getattr(self, h)
        holding_value = holding_df.values
        count = 0
        for i in range(1, len(self.ts)):
            if count == 0 and (holding_value[i] == 0).all():
                continue
            elif count == 0 and (holding_value[i] != 0).any():
                count = 1
            elif count != 0 and count != turnover:
                holding_value[i] = holding_value[i - 1]
                count += 1
            elif count == turnover and (holding_value[i] == 0).all():
                count = 0
            elif count == turnover and (holding_value[i] != 0).any():
                count = 1
            else:
                raise Exception('持仓调整出现错误，请检查')
        for i in np.arange(len(self.asset)):
            setattr(self, self.asset[i], holding_value[:, i])
            self.newest_holdings[self.asset[i]] = holding_value[-1, i]

    # 生成持仓Dataframe
    def to_frame(self):
        holding_df = pd.DataFrame()
        for h in self.asset:
            holding_df[h] = getattr(self, h)
        holding_df.index = self.ts
        return holding_df