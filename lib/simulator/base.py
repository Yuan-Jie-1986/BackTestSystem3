

import sys
import numpy as np
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pprint
import yaml
import os
from pandas.plotting import register_matplotlib_converters



# 单次的交易记录
class TradeRecordByTimes(object):
    def __init__(self):
        self.dt = None  # 交易时间
        self.trade_commodity = None  # 交易商品
        self.trade_contract = None  # 交易合约
        self.trade_direction = None  # 交易方向, 1为做多，-1为做空
        self.trade_price = None  # 交易价格
        self.trade_exchangeRate = None  # 交易时的汇率
        self.trade_volume = None  # 交易量
        self.trade_multiplier = None  # 交易乘数
        self.trade_margin_ratio = None  # 保证金比率
        self.trade_margin_occupation = None  # 保证金占用
        self.trade_type = None  # 交易类型，是平仓还是开仓。1为开仓，-1为平仓
        self.trade_commodity_value = None  # 合约价值，一定是正值
        self.trade_cost_mode = None  # 手续费收取方式：percentage还是fixed
        self.trade_cost_unit = None  # 手续费
        self.trade_cost = 0.

    def setDT(self, val):
        self.dt = val

    def setPrice(self, val):
        self.trade_price = val

    def setExchangRate(self, val):
        self.trade_exchangeRate = val

    def setCommodity(self, val):
        self.trade_commodity = val

    def setContract(self, val):
        self.trade_contract = val

    def setDirection(self, val):
        self.trade_direction = val

    def setType(self, val):
        self.trade_type = val

    def setVolume(self, val):
        self.trade_volume = val

    def setMultiplier(self, val):
        self.trade_multiplier = val

    def setMarginRatio(self, val):
        self.trade_margin_ratio = val

    def setCost(self, mode, value):
        self.trade_cost_mode = mode
        self.trade_cost_unit = value

    def calMarginOccupation(self):
        self.trade_margin_occupation = self.trade_price * self.trade_multiplier * self.trade_margin_ratio * \
                                       self.trade_volume * self.trade_exchangeRate

    def calValue(self):
        self.trade_commodity_value = self.trade_price * self.trade_multiplier * abs(self.trade_volume) * \
                                     self.trade_exchangeRate

    def calCost(self):
        if self.trade_cost_mode == 'percentage':
            self.trade_cost = self.trade_price * self.trade_volume * self.trade_multiplier * self.trade_cost_unit * \
                              self.trade_exchangeRate
        elif self.trade_cost_mode == 'fixed':
            self.trade_cost = self.trade_volume * self.trade_cost_unit


# 逐笔的交易记录
class TradeRecordByTrade(object):

    def __init__(self):
        self.open = np.nan
        self.open_dt = None
        self.close = np.nan
        self.close_dt = None
        self.volume = np.nan
        self.direction = np.nan
        self.contract = ''
        self.commodity = ''
        self.multiplier = np.nan
        self.count = np.nan
        self.pnl = np.nan
        self.rtn = np.nan
        self.holding_period = np.nan  # 该交易的交易周期
        self.tcost_mode = None
        self.tcost_unit = np.nan
        self.tcost = 0
        self.open_exchange_rate = 1.
        self.close_exchange_rate = 1.

    def calcPnL(self):
        self.pnl = (self.close * self.close_exchange_rate - self.open * self.open_exchange_rate) * self.volume *\
                   self.multiplier * self.direction - self.tcost

    def calcRtn(self):
        self.calcPnL()
        self.rtn = self.pnl / (self.open * self.volume * self.multiplier * self.open_exchange_rate)
        # self.rtn = self.direction * ((self.close / self.open) - 1.)

    def calcHoldingPeriod(self):
        self.holding_period = (self.close_dt - self.open_dt + timedelta(1)).days

    def calcTcost(self):
        if self.tcost_mode == 'percentage':
            self.tcost = (self.open + self.close) * self.volume * self.multiplier * self.tcost_unit
        elif self.tcost_mode == 'fixed':
            self.tcost = self.volume * 2. * self.tcost_unit

    def setOpen(self, val):
        self.open = val

    def setOpenDT(self, val):
        self.open_dt = val

    def setClose(self, val):
        self.close = val

    def setCloseDT(self, val):
        self.close_dt = val

    def setVolume(self, val):
        self.volume = val

    def setDirection(self, val):
        self.direction = val

    def setContract(self, val):
        self.contract = val

    def setCommodity(self, val):
        self.commodity = val

    def setMultiplier(self, val):
        self.multiplier = val

    def setCounter(self, val):
        self.count = val

    def setTcost(self, mode, value):
        self.tcost_mode = mode
        self.tcost_unit = value

    def setOpenExchangeRate(self, val):
        self.open_exchange_rate = val

    def setCloseExchangeRate(self, val):
        self.close_exchange_rate = val


# 逐日的交易记录
class TradeRecordByDay(object):

    def __init__(self, dt, holdPosDict, MkData, newTrade):
        self.dt = dt  # 日期
        self.newTrade = newTrade  # 当天进行的交易
        self.mkdata = MkData  # 合约市场数据
        self.holdPosition = holdPosDict  # 之前已有的持仓, 字典中的volume的值是有正有负，正值代表持多仓，负值为空仓
        self.daily_pnl = 0  # 每日的pnl
        self.daily_margin_occ = 0  # 每日的保证金占用情况
        self.daily_commodity_value = 0  # 每日的合约价值

    def addNewPositon(self):

        for tObj in self.newTrade:

            if tObj.dt != self.dt:
                continue

            if tObj.trade_contract not in self.holdPosition:
                self.holdPosition[tObj.trade_contract] = dict()

            self.holdPosition[tObj.trade_contract].setdefault('newTrade', []).append(tObj)

    def getFinalMK(self):

        for k, v in list(self.holdPosition.items()):
            if 'volume' in v:
                if k not in self.mkdata:
                    print('%s合约%s没有数据' % (k, self.dt.strftime('%Y%m%d')))
                    continue
                if 'PRECLOSE' not in self.mkdata[k]:
                    print(self.dt, self.mkdata[k])
                    raise Exception('请检查传入的市场数据是否正确')

                new_pnl = v['volume'] * (self.mkdata[k]['CLOSE'] * self.mkdata[k]['ExRate'] - self.mkdata[k]['PRECLOSE']
                                         * self.mkdata[k]['PRECLOSE_ExRate']) * self.mkdata[k]['multiplier']

                # 如果某些品种当天没有成交量，那么算出来的结果可能为nan
                if np.isnan(new_pnl):
                    print(self.dt, self.mkdata[k])
                    raise Exception('请检查当天的量价数据是否有问题')

                self.daily_pnl += new_pnl

            else:
                self.holdPosition[k]['volume'] = 0

            if 'newTrade' in v:

                for nt in v['newTrade']:

                    new_pnl = nt.trade_volume * nt.trade_direction * nt.trade_multiplier * \
                              (self.mkdata[k]['CLOSE'] * self.mkdata[k]['ExRate'] - nt.trade_price * nt.trade_exchangeRate)

                    # 如果某些品种当天没有成交量，那么算出来的结果可能为nan
                    if np.isnan(new_pnl):
                        print(self.dt, nt.__dict__)
                        raise Exception('请检查当天的量价数据是否有问题')
                    if np.isnan(nt.trade_cost):
                        print(self.dt, nt.__dict__)
                        raise Exception('交易费用为nan，请检查当天的量价数据是否有问题')

                    self.daily_pnl = self.daily_pnl + new_pnl - nt.trade_cost
                    self.holdPosition[k]['volume'] = self.holdPosition[k]['volume'] + nt.trade_volume * \
                                                     nt.trade_direction

                del self.holdPosition[k]['newTrade']

            if self.holdPosition[k]['volume'] == 0:
                del self.holdPosition[k]

            else:
                new_margin_occ = abs(self.holdPosition[k]['volume']) * self.mkdata[k]['CLOSE'] * self.mkdata[k]['ExRate'] * \
                                 self.mkdata[k]['multiplier'] * self.mkdata[k]['margin_ratio']
                new_commodity_value = abs(self.holdPosition[k]['volume']) * self.mkdata[k]['CLOSE'] * self.mkdata[k]['ExRate'] * \
                                      self.mkdata[k]['multiplier']

                self.daily_margin_occ += new_margin_occ
                self.daily_commodity_value += new_commodity_value

        return self.daily_pnl, self.daily_margin_occ, self.daily_commodity_value

    def getHoldPosition(self):
        return self.holdPosition


class BacktestSys(object):

    def __init__(self):
        self.current_file = sys.argv[0]
        self.prepare()

    def prepare(self):
        # 根据yaml配置文件从数据库中抓取数据，并对回测进行参数设置
        current_yaml = '.'.join((os.path.splitext(self.current_file)[0], 'yaml'))
        f = open(current_yaml, encoding='utf-8')
        conf = yaml.load(f, Loader=yaml.FullLoader)

        # 回测起始时间
        self.start_dt = datetime.strptime(conf['start_date'], '%Y%m%d')

        # 回测结束时间
        self.end_dt = datetime.strptime(conf['end_date'], '%Y%m%d')

        # 初始资金
        self.capital = np.float(conf['capital'])

        # 数据库的配置信息
        host = conf['host']
        port = conf['port']
        usr = conf['user']
        pwd = conf['pwd']
        db_nm = conf['db_name']

        conn = pymongo.MongoClient(host=host, port=port)
        self.db = conn[db_nm]
        self.db.authenticate(name=usr, password=pwd)

        # 将需要的数据信息存到字典self.data中，key是各个合约，value仍然是个字典，里面包含需要的字段信息
        # e.g. self.data = {'TA.CZC': {'CLOSE': [], 'date': []}}

        raw_data = conf['data']
        self.data = {}
        self.unit_change = {}  # 计量单位是否需要进行转换
        self.unit = conf['trade_unit']
        self.bt_mode = conf['backtest_mode']
        self.margin_ratio = conf['margin_ratio']
        self.category = {}
        self.tcost = conf['tcost']
        self.switch_contract = conf['switch_contract']
        self.turnover = conf['turnover']
        self.exchange_dict = {}

        if self.tcost:
            self.tcost_list = conf['tcost_list']

        for d in raw_data:
            self.category[d['obj_content']] = d['commodity']
            table = self.db[d['collection']] if 'collection' in d else self.db['FuturesMD']
            query_arg = {d['obj_field']: d['obj_content'], 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date'] + d['fields']
            if self.switch_contract:
                projection_fields = projection_fields + ['switch_contract', 'specific_contract']
            res = table.find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            self.data[d['obj_content']] = df_res.to_dict(orient='list')
            self.unit_change[d['obj_content']] = d['unit_change'] if 'unit_change' in d else 'rmb'

        # 如果需要导入美元兑人民币汇率
        if 'dollar' in self.unit_change.values():
            query_arg = {'wind_code': 'M0067855', 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date', 'CLOSE']
            res = self.db['EDB'].find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            self.dollar2rmb = df_res.to_dict(orient='list')
            self.exchange_dict['dollar'] = 'dollar2rmb'



        # 将提取的数据按照交易时间的并集重新生成
        date_set = set()
        for _, v in self.data.items():
            date_set = date_set.union(v['date'])
        self.dt = np.array(list(date_set))
        self.dt.sort()

        # 如果定义了date_type，则去调取交易日期序列
        if 'date_type' in conf:
            self.date_type = conf['date_type']
            table = self.db['DateDB']
            query_arg = {'exchange': self.date_type, 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date']
            res = table.find(query_arg, projection_fields)
            df_res = pd.DataFrame.from_records(res)
            trading_dt = df_res['date'].values
            trading_dt = np.array([pd.Timestamp(dt) for dt in trading_dt])
            dt_con = np.in1d(self.dt, trading_dt)
            self.dt = self.dt[dt_con]


        # 根据交易日期序列重新整理数据
        for k, v in self.data.items():
            con_1 = np.in1d(self.dt, v['date'])
            con_2 = np.in1d(v['date'], self.dt)
            self.data[k].pop('date')
            for sub_k, sub_v in v.items():
                if sub_k == 'date':
                    continue
                if sub_k == 'specific_contract':
                    self.data[k][sub_k] = np.array(len(self.dt) * [None])
                    self.data[k][sub_k][con_1] = np.array(sub_v)[con_2]
                else:
                    self.data[k][sub_k] = np.ones(self.dt.shape) * np.nan
                    self.data[k][sub_k][con_1] = np.array(sub_v)[con_2]

        # 根据交易日期序列重新整理汇率
        for k in self.exchange_dict:
            con_1 = np.in1d(self.dt, getattr(self, self.exchange_dict[k])['date'])
            con_2 = np.in1d(getattr(self, self.exchange_dict[k])['date'], self.dt)
            getattr(self, self.exchange_dict[k]).pop('date')
            close_new = np.ones(self.dt.shape) * np.nan
            close_new[con_1] = np.array(getattr(self, self.exchange_dict[k])['CLOSE'])[con_2]

            close_new = pd.DataFrame(close_new).fillna(method='ffill').values.flatten()

            # 对于汇率来说，如果出现nan值会影响计算，这里需要进行判断
            if np.isnan(close_new).any():
                print('%s出现了nan值，使用向前填充' % k)
                print(self.dt[np.isnan(close_new)])
                close_new = pd.DataFrame(close_new).fillna(method='bfill').values.flatten()


            getattr(self, self.exchange_dict[k])['CLOSE'] = close_new

        if 'rmb' in self.unit_change.values():
            self.rmb = {'CLOSE': np.ones(len(self.dt))}
            self.exchange_dict['rmb'] = 'rmb'



        # # 对当天没有交易的品种的OPEN进行修正处理
        # for k in self.data:
        #     try:
        #         self.data[k]['OPEN'][np.isnan(self.data[k]['OPEN'])] = self.data[k]['CLOSE'][np.isnan(self.data[k]['OPEN'])]
        #     except KeyError:
        #         continue

    def strategy(self):
        raise NotImplementedError

    def wgtsProcess(self, wgtsDict):
        # 对生成的权重进行处理，需要注意的是这个函数要最后再用
        if self.bt_mode == 'OPEN':
            # 如果是开盘价进行交易，则将初始权重向后平移一位
            for k in wgtsDict:
                res = np.zeros(len(wgtsDict[k]) + 1)
                res[1:] = wgtsDict[k]
                wgtsDict[k] = res

        # 如果在配置文件中的持仓周期大于1，需要对持仓进行调整。通常该参数是针对alpha策略。
        if self.turnover > 1:
            print('根据turnover对持仓进行调整，请检查是否为alpha策略')
            wgts_df = pd.DataFrame.from_dict(wgtsDict)
            wgts_value = wgts_df.values
            wgts_index = wgts_df.index
            wgts_columns = wgts_df.columns

            count = 0
            for i in range(1, len(self.dt)):

                if count == 0 and (wgts_value[i] == 0).all():
                    continue
                elif count == 0 and (wgts_value[i] != 0).any():
                    count = 1
                elif count != 0 and count != self.turnover:
                    wgts_value[i] = wgts_value[i-1]
                    count += 1
                elif count == self.turnover and (wgts_value[i] == 0).all():
                    count = 0
                elif count == self.turnover and (wgts_value[i] != 0).any():
                    count = 1
                else:
                    raise Exception('持仓调整出现错误，请检查')

            wgts_df = pd.DataFrame(wgts_value, index=wgts_index, columns=wgts_columns)

            wgtsDict = wgts_df.to_dict(orient='list')

        # 如果当天合约没有交易量，那么需要对权重进行调整
        for k in wgtsDict:
            for i in range(1, len(self.dt)):
                if np.isnan(self.data[k]['CLOSE'][:i+1]).all():
                    continue
                if np.isnan(self.data[k]['CLOSE'][i]) or ('OPEN' in self.data[k] and np.isnan(self.data[k]['OPEN'][i])):
                    print('%s合约在%s这一天没有成交，对持仓权重进行了调整，调整前是%f，调整后是%f' % \
                          (k, self.dt[i].strftime('%Y%m%d'), wgtsDict[k][i], wgtsDict[k][i-1]))
                    wgtsDict[k][i] = wgtsDict[k][i-1]

        return wgtsDict

    def wgtsStandardization(self, wgtsDict, mode=0):
        """根据给定的持仓重新生成标准化的持仓：
        mode=0：不加杠杆。所有持仓品种的合约价值相同。每天的持仓情况会根据每日的收盘价而发生变化。这样会产生一些不必要的交易。
                价格越低，越增加持仓，价格越高，越减少持仓。
        mode=1: 不加杠杆。所有持仓品种的合约价值相同。如果某个品种的初始持仓没有变化，那就不调整。某个品种持仓有变化，就根据剩余
                资金按照合约价值来分配持仓。
        mode=2: 不加杠杆。所有持仓品种按照其波动率进行调整，按照相对的波动来对持仓进行分配。
        mode=3: 不加杠杆。所有持仓品种按照ATR进行调整，按照ATR来对持仓进行分配。计算ATR时需要最高价最低价
        """
        # 合约持仓的dataframe
        wgts_df = pd.DataFrame.from_dict(wgtsDict)
        wgts_df.index = self.dt

        # 计算出各合约做1手的合约价值
        cls = {}
        for k in wgtsDict:
            cls[k] = self.data[k]['CLOSE'] * self.unit[self.category[k]]
            cls[k] = cls[k] * getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE']
        cls_df = pd.DataFrame.from_dict(cls)
        cls_df.index = self.dt

        if mode == 0 or mode == 1:

            # 根据持仓得到每日的持有几个合约，针对每个合约平均分配资金
            wgts_num = np.abs(np.sign(wgts_df))
            wgts_num = wgts_num.sum(axis=1)
            wgts_num[wgts_num == 0] = np.nan
            sub_capital = self.capital / wgts_num

            cls_df = cls_df * np.sign(wgts_df)
            cls_df[cls_df == 0] = np.nan

            wgts_new = pd.DataFrame()
            for c in cls_df:
                wgts_new[c] = sub_capital / cls_df[c]

            wgts_new.fillna(0, inplace=True)

            if mode == 0:
                wgts_new = wgts_new.round(decimals=0)
                wgts_new = wgts_new.to_dict(orient='list')
                return wgts_new

            elif mode == 1:
                # 判断初始持仓是否与前一天的初始持仓相同
                wgts_yestd = wgts_df.shift(periods=1)
                wgts_equal = wgts_df == wgts_yestd

                # 统计当天持仓的合约个数
                wgts_temp = wgts_df.copy()
                wgts_temp[wgts_temp == 0] = np.nan
                wgts_num = wgts_temp.count(axis=1)
                wgts_num_yestd = wgts_num.shift(periods=1)

                # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
                num_equal = wgts_num == wgts_num_yestd
                wgts_equal.loc[~num_equal] = False
                wgts_new[wgts_equal] = np.nan
                wgts_new.fillna(method='ffill', inplace=True)
                wgts_new = wgts_new.round(decimals=0)
                wgts_new = wgts_new.to_dict(orient='list')

                return wgts_new

        elif mode == 2:
            # 计算各合约过去一年的合约价值的标准差
            std_df = cls_df.rolling(window=250, min_periods=200).std()
            # 根据合约价值的标准差的倒数的比例来分配资金
            ratio_df = self.capital / std_df
            ratio_df[wgts_df == 0] = np.nan
            ratio_total = ratio_df.sum(axis=1)
            sub_capital = pd.DataFrame()
            for c in ratio_df:
                sub_capital[c] = ratio_df[c] / ratio_total * self.capital

            # 根据分配资金进行权重的重新生成
            wgts_new = sub_capital / cls_df * np.sign(wgts_df)
            wgts_new.fillna(0, inplace=True)

            # 如果初始权重不变，则不对权重进行调整
            # 判断初始持仓是否与前一天的初始持仓相同
            wgts_yestd = wgts_df.shift(periods=1)
            wgts_equal = wgts_df == wgts_yestd

            # 统计当天持仓的合约个数
            wgts_temp = wgts_df.copy()
            wgts_temp[wgts_temp == 0] = np.nan
            wgts_num = wgts_temp.count(axis=1)
            wgts_num_yestd = wgts_num.shift(periods=1)

            # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
            num_equal = wgts_num == wgts_num_yestd
            wgts_equal.loc[~num_equal] = False
            wgts_new[wgts_equal] = np.nan
            wgts_new.fillna(method='ffill', inplace=True)
            wgts_new = wgts_new.round(decimals=0)
            wgts_new = wgts_new.to_dict(orient='list')
            return wgts_new

        elif mode == 3:
            # 使用ATR，需要最高价最低价，否则会报错
            cls_atr = {}
            high_atr = {}
            low_atr = {}
            for k in wgtsDict:
                cls_atr[k] = self.data[k]['CLOSE'] * self.unit[self.category[k]]
                cls_atr[k] = cls_atr[k] * getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE']
                high_atr[k] = self.data[k]['HIGH'] * self.unit[self.category[k]]
                high_atr[k] = high_atr[k] * getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE']
                low_atr[k] = self.data[k]['LOW'] * self.unit[self.category[k]]
                low_atr[k] = low_atr[k] * getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE']

            cls_atr_df = pd.DataFrame.from_dict(cls_atr)
            cls_atr_df.index = self.dt
            high_atr_df = pd.DataFrame.from_dict(high_atr)
            high_atr_df.index = self.dt
            low_atr_df = pd.DataFrame.from_dict(low_atr)
            low_atr_df.index = self.dt

            cls_atr_yestd_df = cls_atr_df.shift(periods=1)

            p1 = high_atr_df - low_atr_df
            p2 = np.abs(high_atr_df - cls_atr_yestd_df)
            p3 = np.abs(cls_atr_yestd_df - low_atr_df)

            true_range = np.maximum(p1, np.maximum(p2, p3))
            atr = pd.DataFrame(true_range).rolling(window=250, min_periods=200).mean()
            # 根据合约价值的ATR的倒数的比例来分配资金
            ratio_df = self.capital / atr
            ratio_df[wgts_df == 0] = np.nan
            ratio_total = ratio_df.sum(axis=1)
            sub_capital = pd.DataFrame()
            for c in ratio_df:
                sub_capital[c] = ratio_df[c] / ratio_total * self.capital

            # 根据分配资金进行权重的重新生成
            wgts_new = sub_capital / cls_df * np.sign(wgts_df)
            wgts_new.fillna(0, inplace=True)

            # 如果初始权重不变，则不对权重进行调整
            # 判断初始持仓是否与前一天的初始持仓相同
            wgts_yestd = wgts_df.shift(periods=1)
            wgts_equal = wgts_df == wgts_yestd

            # 统计当天持仓的合约个数
            wgts_temp = wgts_df.copy()
            wgts_temp[wgts_temp == 0] = np.nan
            wgts_num = wgts_temp.count(axis=1)
            wgts_num_yestd = wgts_num.shift(periods=1)

            # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
            num_equal = wgts_num == wgts_num_yestd
            wgts_equal.loc[~num_equal] = False
            wgts_new[wgts_equal] = np.nan
            wgts_new.fillna(method='ffill', inplace=True)
            wgts_new = wgts_new.round(decimals=0)
            wgts_new = wgts_new.to_dict(orient='list')
            return wgts_new

    def getPnlDaily(self, wgtsDict):
        # 根据权重计算每日的pnl，每日的保证金占用，每日的合约价值
        # wgtsDict是权重字典，key是合约名，value是该合约权重
        # 检查权重向量与时间向量长度是否一致
        # 需要注意的问题是传入的参数如果是字典，那么可能会改变该字典
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print('%s的权重向量与时间向量长度不一致，%s的权重向量长度为%d，时间向量的长度为%d' % (k, k, len(v), len(self.dt)))
                raise Exception('权重向量与时间向量长度不一致')
        pnl_daily = np.zeros_like(self.dt).astype('float')
        margin_occ_daily = np.zeros_like(self.dt).astype('float')
        value_daily = np.zeros_like(self.dt).astype('float')

        holdpos = {}

        for i, v in enumerate(self.dt):
            newtradedaily = []
            mkdata = {}
            for k, val in wgtsDict.items():

                # 如果在当前日期该合约没有开始交易，则直接跳出当前循环，进入下一个合约

                if np.isnan(self.data[k]['CLOSE'][:i+1]).all():
                    continue

                if np.isnan(self.data[k]['CLOSE'][i]):
                    print('%s合约在%s这一天没有收盘数据' % (k, v.strftime('%Y%m%d')))
                    continue

                # 需要传入的市场数据

                mkdata[k] = {'CLOSE': self.data[k]['CLOSE'][i],
                             'ExRate': getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i],
                             'multiplier': self.unit[self.category[k]],
                             'margin_ratio': self.margin_ratio[k]}

                # 合约首日交易便有持仓时
                if i == 0 or np.isnan(self.data[k]['CLOSE'][:i]).all():
                    if val[i] != 0:
                        newtrade = TradeRecordByTimes()
                        newtrade.setDT(v)
                        newtrade.setContract(k)
                        newtrade.setCommodity(self.category[k])
                        newtrade.setPrice(self.data[k][self.bt_mode][i])
                        newtrade.setExchangRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                        newtrade.setType(1)
                        newtrade.setVolume(abs(val[i]))
                        newtrade.setMultiplier(self.unit[self.category[k]])
                        newtrade.setDirection(np.sign(val[i]))
                        if self.tcost:
                            newtrade.setCost(**self.tcost_list[k])
                        newtrade.calCost()
                        newtradedaily.append(newtrade)
                # 如果不是第一天交易的话，需要前一天的收盘价
                elif i != 0:
                    mkdata[k]['PRECLOSE'] = self.data[k]['CLOSE'][i - 1]
                    mkdata[k]['PRECLOSE_ExRate'] = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i - 1]
                    if np.isnan(mkdata[k]['PRECLOSE']):
                        for pre_counter in np.arange(2, i + 1):
                            if ~np.isnan(self.data[k]['CLOSE'][i - pre_counter]):
                                mkdata[k]['PRECLOSE'] = self.data[k]['CLOSE'][i - pre_counter]
                                mkdata[k]['PRECLOSE_ExRate'] = getattr(
                                    self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i - pre_counter]
                                print('%s合约在%s使用的PRECLOSE是%d天前的收盘价' % (k, self.dt[i].strftime('%Y%m%d'), pre_counter))
                                break

                    # 如果切换主力合约
                    if self.switch_contract:
                        mkdata[k].update({'switch_contract': self.data[k]['switch_contract'][i],
                                          'specific_contract': self.data[k]['specific_contract'][i - 1]})
                        # 如果switch_contract为True，需要前主力合约的OPEN

                        if mkdata[k]['switch_contract'] and ~np.isnan(mkdata[k]['switch_contract']):
                            # 这里需要注意的是np.nan也会判断为true，所以需要去掉
                            # 比如MA.CZC在刚开始的时候是没有specific_contract

                            queryArgs = {'wind_code': mkdata[k]['specific_contract'], 'date': self.dt[i]}
                            projectionField = ['OPEN']
                            table = self.db['FuturesMD']

                            if mkdata[k]['specific_contract'] == 'nan':
                                # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                                print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                                      (mkdata[k], self.dt[i].strftime('%Y%m%d')))
                                old_open = self.data[k]['CLOSE'][i - 1]
                                old_open_exrate = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i - 1]
                            elif table.find_one(queryArgs, projectionField):
                                old_open = table.find_one(queryArgs, projectionField)['OPEN']
                                old_open_exrate = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i]
                                if np.isnan(old_open):
                                    print('%s因为该合约当天没有交易，在%s使用前一天的收盘价作为换约平仓的价格' % \
                                          (mkdata[k]['specific_contract'], self.dt[i].strftime('%Y%m%d')))
                                    old_open = mkdata[k]['PRECLOSE']
                                    old_open_exrate = mkdata[k]['PRECLOSE_ExRate']
                            else:
                                print('%s因为已经到期，在%s使用的是前一天的收盘价作为换约平仓的价格' % \
                                      (mkdata[k]['specific_contract'], self.dt[i].strftime('%Y%m%d')))
                                old_open = mkdata[k]['PRECLOSE']
                                old_open_exrate = mkdata[k]['PRECLOSE_ExRate']

                    if self.switch_contract and mkdata[k]['switch_contract'] and ~np.isnan(mkdata[k]['switch_contract'])\
                            and val[i-1] != 0:
                        newtrade1 = TradeRecordByTimes()
                        newtrade1.setDT(v)
                        newtrade1.setContract(k)
                        newtrade1.setCommodity(self.category[k])
                        newtrade1.setPrice(old_open)
                        newtrade1.setExchangRate(old_open_exrate)
                        newtrade1.setVolume(abs(val[i-1]))
                        newtrade1.setMultiplier(self.unit[self.category[k]])
                        newtrade1.setDirection(-np.sign(val[i-1]))
                        if self.tcost:
                            newtrade1.setCost(**self.tcost_list[k])
                        newtrade1.calCost()
                        newtradedaily.append(newtrade1)

                        if val[i] != 0:
                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setDT(v)
                            newtrade2.setContract(k)
                            newtrade2.setCommodity(self.category[k])
                            newtrade2.setPrice(self.data[k][self.bt_mode][i])
                            newtrade2.setExchangRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(val[i]))
                            newtrade2.setMultiplier(self.unit[self.category[k]])
                            newtrade2.setDirection(np.sign(val[i]))
                            if self.tcost:
                                newtrade2.setCost(**self.tcost_list[k])
                            newtrade2.calCost()
                            newtradedaily.append(newtrade2)

                    else:
                        if val[i] * val[i-1] < 0:
                            newtrade1 = TradeRecordByTimes()
                            newtrade1.setDT(v)
                            newtrade1.setContract(k)
                            newtrade1.setCommodity(self.category[k])
                            newtrade1.setPrice(self.data[k][self.bt_mode][i])
                            newtrade1.setExchangRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                            newtrade1.setType(-1)
                            newtrade1.setVolume(abs(val[i-1]))
                            newtrade1.setMultiplier(self.unit[self.category[k]])
                            newtrade1.setDirection(np.sign(val[i]))
                            if self.tcost:
                                newtrade1.setCost(**self.tcost_list[k])
                            newtrade1.calCost()
                            newtradedaily.append(newtrade1)

                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setDT(v)
                            newtrade2.setContract(k)
                            newtrade2.setCommodity(self.category[k])
                            newtrade2.setPrice(self.data[k][self.bt_mode][i])
                            newtrade2.setExchangRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(val[i]))
                            newtrade2.setMultiplier(self.unit[self.category[k]])
                            newtrade2.setDirection(np.sign(val[i]))
                            if self.tcost:
                                newtrade2.setCost(**self.tcost_list[k])
                            newtrade2.calCost()
                            newtradedaily.append(newtrade2)

                        elif val[i] == val[i-1]:  # 没有交易
                            continue

                        else:
                            newtrade = TradeRecordByTimes()
                            newtrade.setDT(v)
                            newtrade.setContract(k)
                            newtrade.setCommodity(self.category[k])
                            newtrade.setPrice(self.data[k][self.bt_mode][i])
                            newtrade.setExchangRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                            newtrade.setType(np.sign(abs(val[i]) - abs(val[i-1])))
                            newtrade.setVolume(abs(val[i] - val[i-1]))
                            newtrade.setMultiplier(self.unit[self.category[k]])
                            newtrade.setDirection(np.sign(val[i] - val[i-1]))
                            if self.tcost:
                                newtrade.setCost(**self.tcost_list[k])
                            newtrade.calCost()
                            newtradedaily.append(newtrade)

            trd = TradeRecordByDay(dt=v, holdPosDict=holdpos, MkData=mkdata, newTrade=newtradedaily)
            trd.addNewPositon()
            pnl_daily[i], margin_occ_daily[i], value_daily[i] = trd.getFinalMK()
            holdpos = trd.getHoldPosition()

        return pnl_daily, margin_occ_daily, value_daily

    def getNV(self, wgtsDict):
        # 计算总的资金曲线变化情况
        return self.capital + np.cumsum(self.getPnlDaily(wgtsDict)[0])

    def statTrade(self, wgtsDict):
        """
        对每笔交易进行统计，wgtsDict是权重字典

        """
        # 检查权重向量与时间长度是否一致
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print('%s的权重向量与时间向量长度不一致' % k)
                raise Exception(u'权重向量与时间向量长度不一致')
        total_pnl = 0.
        trade_record = {}
        uncovered_record = {}
        for k, v in wgtsDict.items():

            trade_record[k] = []
            uncovered_record[k] = []
            count = 0
            trade_price = self.data[k][self.bt_mode]

            for i in range(len(v)):

                if i == 0 and v[i] == 0:
                    continue
                # if np.isnan(trade_price[i]):
                #     # 需要注意的是如果当天没有成交量，可能一些价格会是nan值，会导致回测计算结果不准确
                #     # 如果当天没有交易量的话，所持有的仓位修改成与单一个交易日相同
                #     v[i] = v[i-1]
                #     continue

                # 如果当天涉及到移仓，需要将昨天的仓位先平掉，然后在新的主力合约上开仓，统一以开盘价平掉旧的主力合约
                if self.switch_contract and self.data[k]['switch_contract'][i] \
                    and ~np.isnan(self.data[k]['switch_contract'][i]) and v[i - 1] != 0:

                    # 条件中需要加上判断合约是否为nan，否则也会进入到该条件中

                    table = self.db['FuturesMD']
                    res = self.data[k]['specific_contract'][i-1]
                    # 对于换合约需要平仓的合约均使用开盘价进行平仓
                    queryArgs = {'wind_code': res, 'date': self.dt[i]}
                    projectionField = ['OPEN']

                    if res == 'nan':
                        # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                        print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                              (k, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = self.data[k]['CLOSE'][i-1]
                        trade_exrate_switch = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i-1]
                    elif table.find_one(queryArgs, projectionField):
                        trade_price_switch = table.find_one(queryArgs, projectionField)['OPEN']
                        trade_exrate_switch = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i]
                        if np.isnan(trade_price_switch):
                            print('%s因为该合约当天没有交易，在%s使用前一天的收盘价作为换约平仓的价格' % \
                                  (res, self.dt[i].strftime('%Y%m%d')))
                            trade_price_switch = self.data[k]['CLOSE'][i-1]
                            trade_exrate_switch = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i - 1]
                    else:
                        print('%s因为已经到期，在%s使用的是前一天的收盘价作为换约平仓的价格' % \
                              (res, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = self.data[k]['CLOSE'][i-1]
                        trade_exrate_switch = getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i - 1]

                    if uncovered_record[k]:
                        needed_covered = abs(v[i - 1])
                        uncovered_record_sub = []
                        for j in np.arange(1, len(uncovered_record[k]) + 1):
                            for m in np.arange(1, len(trade_record[k]) + 1):
                                if uncovered_record[k][-j] == trade_record[k][-m].count:
                                    trade_record[k][-m].setClose(trade_price_switch)
                                    trade_record[k][-m].setCloseExchangeRate(trade_exrate_switch)
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])
                                    needed_covered -= trade_record[k][-m].volume
                        if needed_covered == 0:
                            for tr in uncovered_record_sub:
                                uncovered_record[k].remove(tr)
                        else:
                            print(self.dt[i], k, uncovered_record[k])
                            raise Exception('仓位没有完全平掉，请检查')

                        if v[i] != 0:
                            # 对新的主力合约进行开仓
                            count += 1
                            tr_r = TradeRecordByTrade()
                            tr_r.setCounter(count)
                            tr_r.setOpen(trade_price[i])
                            tr_r.setOpenExchangeRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                            tr_r.setOpenDT(self.dt[i])
                            tr_r.setCommodity(self.category[k])
                            tr_r.setVolume(abs(v[i]))
                            tr_r.setMultiplier(self.unit[self.category[k]])
                            tr_r.setContract(k)
                            tr_r.setDirection(np.sign(v[i]))
                            if self.tcost:
                                tr_r.setTcost(**self.tcost_list[k])
                            trade_record[k].append(tr_r)
                            uncovered_record[k].append(tr_r.count)

                else:
                    if (v[i] != 0 and i == 0) or (v[i] != 0 and np.isnan(trade_price[:i]).all()):
                        # 第一天交易就开仓
                        # 第二种情况是为了排除该品种当天没有交易，价格为nan的这种情况，e.g. 20141202 BU.SHF
                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(self.category[k])
                        tr_r.setVolume(abs(v[i]))
                        tr_r.setMultiplier(self.unit[self.category[k]])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(v[i]))
                        if self.tcost:
                            tr_r.setTcost(**self.tcost_list[k])
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)

                    elif abs(v[i]) > abs(v[i-1]) and v[i] * v[i-1] >= 0:
                        # 新开仓或加仓

                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(self.category[k])
                        tr_r.setVolume(abs(v[i]) - abs(v[i-1]))
                        tr_r.setMultiplier(self.unit[self.category[k]])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(v[i]))
                        if self.tcost:
                            tr_r.setTcost(**self.tcost_list[k])
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)


                    elif abs(v[i]) < abs(v[i-1]) and v[i] * v[i-1] >= 0:

                        # 减仓或平仓
                        needed_covered = abs(v[i - 1]) - abs(v[i])  # 需要减仓的数量
                        uncovered_record_sub = []
                        uncovered_record_add = []
                        for j in np.arange(1, len(uncovered_record[k]) + 1):
                            for m in np.arange(1, len(trade_record[k]) + 1):
                                if uncovered_record[k][-j] == trade_record[k][-m].count:
                                    # 如果需要减仓的数量小于最近的开仓的数量
                                    if needed_covered < trade_record[k][-m].volume:
                                        uncovered_vol = trade_record[k][-m].volume - needed_covered
                                        trade_record[k][-m].setVolume(needed_covered)
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                                        trade_record[k][-m].setCloseDT(self.dt[i])
                                        trade_record[k][-m].calcHoldingPeriod()
                                        trade_record[k][-m].calcTcost()
                                        trade_record[k][-m].calcPnL()
                                        trade_record[k][-m].calcRtn()
                                        uncovered_record_sub.append(uncovered_record[k][-j])

                                        needed_covered = 0.

                                        # 对于没有平仓的部分新建交易记录
                                        count += 1
                                        tr_r = TradeRecordByTrade()
                                        tr_r.setCounter(count)
                                        tr_r.setOpen(trade_record[k][-m].open)
                                        tr_r.setOpenExchangeRate(trade_record[k][-m].open_exchange_rate)
                                        tr_r.setOpenDT(trade_record[k][-m].open_dt)
                                        tr_r.setCommodity(self.category[k])
                                        tr_r.setVolume(uncovered_vol)
                                        tr_r.setMultiplier(self.unit[self.category[k]])
                                        tr_r.setContract(k)
                                        tr_r.setDirection(trade_record[k][-m].direction)
                                        if self.tcost:
                                            tr_r.setTcost(**self.tcost_list[k])
                                        trade_record[k].append(tr_r)
                                        uncovered_record_add.append(tr_r.count)

                                        break


                                    # 如果需要减仓的数量等于最近的开仓数量
                                    elif needed_covered == trade_record[k][-m].volume:
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                                        trade_record[k][-m].setCloseDT(self.dt[i])
                                        trade_record[k][-m].calcHoldingPeriod()
                                        trade_record[k][-m].calcTcost()
                                        trade_record[k][-m].calcPnL()
                                        trade_record[k][-m].calcRtn()
                                        uncovered_record_sub.append(uncovered_record[k][-j])
                                        needed_covered = 0.
                                        break

                                    # 如果需要减仓的数量大于最近的开仓数量
                                    elif needed_covered > trade_record[k][-m].volume:
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                                        trade_record[k][-m].setCloseDT(self.dt[i])
                                        trade_record[k][-m].calcHoldingPeriod()
                                        trade_record[k][-m].calcTcost()
                                        trade_record[k][-m].calcPnL()
                                        trade_record[k][-m].calcRtn()
                                        uncovered_record_sub.append(uncovered_record[k][-j])
                                        needed_covered -= trade_record[k][-m].volume
                                        break

                            if needed_covered == 0.:
                                for tr in uncovered_record_sub:
                                    uncovered_record[k].remove(tr)
                                for tr in uncovered_record_add:
                                    uncovered_record[k].append(tr)
                                break

                    elif v[i] * v[i-1] < 0:

                        # 先平仓后开仓
                        needed_covered = abs(v[i - 1])  # 需要减仓的数量
                        uncovered_record_sub = []
                        for j in np.arange(1, len(uncovered_record[k]) + 1):
                            for m in np.arange(1, len(trade_record[k]) + 1):
                                if uncovered_record[k][-j] == trade_record[k][-m].count:
                                    # 如果需要减仓的数量小于最近的开仓的数量，会报错
                                    if needed_covered < trade_record[k][-m].volume:
                                        raise Exception('请检查，待减仓的数量为什么会小于已经开仓的数量')

                                    # 如果需要减仓的数量等于最近的开仓数量
                                    elif needed_covered == trade_record[k][-m].volume:
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                                        trade_record[k][-m].setCloseDT(self.dt[i])
                                        trade_record[k][-m].calcHoldingPeriod()
                                        trade_record[k][-m].calcTcost()
                                        trade_record[k][-m].calcPnL()
                                        trade_record[k][-m].calcRtn()
                                        uncovered_record_sub.append(uncovered_record[k][-j])
                                        needed_covered = 0.

                                        break

                                    # 如果需要减仓的数量大于最近的开仓数量
                                    elif needed_covered > trade_record[k][-m].volume:
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                                        trade_record[k][-m].setCloseDT(self.dt[i])
                                        trade_record[k][-m].calcHoldingPeriod()
                                        trade_record[k][-m].calcTcost()
                                        trade_record[k][-m].calcPnL()
                                        trade_record[k][-m].calcRtn()
                                        uncovered_record_sub.append(uncovered_record[k][-j])
                                        needed_covered -= trade_record[k][-m].volume

                                        break

                            if needed_covered == 0.:
                                for tr in uncovered_record_sub:
                                    uncovered_record[k].remove(tr)
                                break
                        if uncovered_record[k]:
                            for trsd in trade_record[k]:
                                print(trsd.__dict__)
                            print(self.dt[i], k, uncovered_record[k])
                            raise Exception('请检查，依然有未平仓的交易，无法新开反向仓')

                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_dict[self.unit_change[k]])['CLOSE'][i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(self.category[k])
                        tr_r.setVolume(abs(v[i]))
                        tr_r.setMultiplier(self.unit[self.category[k]])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(v[i]))
                        if self.tcost:
                            tr_r.setTcost(**self.tcost_list[k])
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)

            trade_times = len(trade_record[k])
            buy_times = len([t for t in trade_record[k] if t.direction == 1])
            sell_times = len([t for t in trade_record[k] if t.direction == -1])
            profit_times = len([t for t in trade_record[k] if t.pnl > 0])
            loss_times = len([t for t in trade_record[k] if t.pnl < 0])
            buy_pnl = [t.pnl for t in trade_record[k] if t.direction == 1]
            sell_pnl = [t.pnl for t in trade_record[k] if t.direction == -1]
            trade_rtn = [t.rtn for t in trade_record[k]]
            trade_holding_period = [t.holding_period for t in trade_record[k]]

            if buy_times == 0:
                buy_avg_pnl = np.nan
            else:
                buy_avg_pnl = np.nansum(buy_pnl) / buy_times
            if sell_times == 0:
                sell_avg_pnl = np.nan
            else:
                sell_avg_pnl = np.nansum(sell_pnl) / sell_times

            print('+++++++++++++++%s合约交易统计++++++++++++++++++++' % k)
            print('交易次数: %d' % trade_times)
            print('做多次数: %d' % buy_times)
            print('做空次数: %d' % sell_times)
            print('盈利次数: %d' % profit_times)
            print('亏损次数: %d' % loss_times)
            print('做多平均盈亏: %f' % buy_avg_pnl)
            print('做空平均盈亏: %f' % sell_avg_pnl)
            print('平均每笔交易收益率(不考虑杠杆): %f' % np.nanmean(trade_rtn))
            print('平均年化收益率(不考虑杠杆): %f' % (np.nansum(trade_rtn) * 250. / np.nansum(trade_holding_period)))

            total_pnl_k = np.nansum([tr.pnl for tr in trade_record[k]])
            total_pnl += total_pnl_k
            # print 'sadfa', total_pnl

        return trade_record

    def displayResult(self, wgtsDict, saveLocal=True):
        # 需要注意的一个问题是，如果在getPnlDaily函数中改变了wgtsDict，那么之后所用的wgtsDict就都改变了
        # saveLocal是逻辑变量，是否将结果存在本地
        if self.bt_mode == 'OPEN':
            new_WgtsDict = {}
            for k in wgtsDict:
                new_WgtsDict[k] = wgtsDict[k][-1]
                wgtsDict[k] = wgtsDict[k][:-1]
        elif self.bt_mode == 'CLOSE':
            new_WgtsDict = {}
            for k in wgtsDict:
                new_WgtsDict[k] = wgtsDict[k][-1]

        pnl, margin_occ, value = self.getPnlDaily(wgtsDict)
        # print 'nv'
        df_pnl = pd.DataFrame(np.cumsum(pnl), index=self.dt)
        df_pnl.to_clipboard()
        nv = 1. + np.cumsum(pnl) / self.capital  # 转换成初始净值为1
        margin_occ_ratio = margin_occ / (self.capital + np.cumsum(pnl))
        # leverage = value / (self.capital + np.cumsum(pnl))
        leverage = value / self.capital
        trade_record = self.statTrade(wgtsDict)
        print('==============================回测结果========================')
        self.calcIndicatorByYear(nv)
        self.showBTResult(nv)

        trade_pnl = []
        for tr in trade_record:
            trade_pnl.extend([t.pnl for t in trade_record[tr]])

        if saveLocal:
            current_file = sys.argv[0]
            save_path = os.path.splitext(current_file)[0]
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            # 保存权重为wgts.csv
            wgts_df = pd.DataFrame.from_dict(wgtsDict)
            wgts_df.index = self.dt
            wgts_df.to_csv(os.path.join(save_path, 'wgts.csv'))

            # 保存总的回测结果为result.csv
            total_df = pd.DataFrame({u'每日PnL': pnl, u'净值': nv, u'资金占用比例': margin_occ_ratio, u'杠杆倍数': leverage},
                                    index=self.dt)
            total_df.to_csv(os.path.join(save_path, 'result.csv'), encoding='utf-8')


            # 保存交易记录为trade_detail.csv
            detail_df = pd.DataFrame()
            for k in trade_record:
                detail_df = pd.concat([detail_df, pd.DataFrame.from_records([tr.__dict__ for tr in trade_record[k]])],
                                      ignore_index=True)
            detail_df.to_csv(os.path.join(save_path, 'details.csv'))

            # 保存最新的权重
            # if self.bt_mode == 'OPEN':
            new_wgt = pd.DataFrame.from_dict(new_WgtsDict, orient='index', columns=['WGTS'])
            new_wgt.sort_values(by='WGTS', ascending=False, inplace=True)
            new_wgt.to_csv(os.path.join(save_path, 'new_wgts_%s.csv' % datetime.now().strftime('%y%m%d')))

        trade_pnl = np.array(trade_pnl)

        register_matplotlib_converters()
        plt.subplot(411)
        plt.plot_date(self.dt, nv, fmt='-r', label='PnL')
        plt.grid()
        plt.legend()

        plt.subplot(412)
        plt.hist(trade_pnl[~np.isnan(trade_pnl)], bins=50, label='DistOfPnL', color='r')
        plt.legend()
        plt.grid()

        plt.subplot(413)
        plt.plot_date(self.dt, margin_occ_ratio, fmt='-r', label='margin_occupation_ratio')
        plt.grid()
        plt.legend()


        plt.subplot(414)
        plt.plot_date(self.dt, leverage, fmt='-r', label='leverage')
        plt.grid()
        plt.legend()

        plt.show()

    def showBTResult(self, net_value):
        rtn, vol, sharpe, dd, dds, dde = self.calcIndicator(net_value)
        print('==============回测结果===============')
        print('年化收益率：', rtn)
        print('年化波动率：', vol)
        print('夏普比率：', sharpe)
        print('最大回撤：', dd)
        print('最大回撤起始时间：', dds)
        print('最大回撤结束时间：', dde)
        print('最终资金净值：', net_value[-1])


    def calcIndicator(self, net_value):
        rtn_daily = np.ones(len(net_value)) * np.nan
        rtn_daily[1:] = net_value[1:] / net_value[:-1] - 1.
        annual_rtn = np.nanmean(rtn_daily) * 250
        annual_std = np.nanstd(rtn_daily) * np.sqrt(250)
        sharpe = annual_rtn / annual_std
        # 最大回撤
        index_end = np.argmax(np.maximum.accumulate(net_value) - net_value)
        index_start = np.argmax(net_value[:index_end])
        max_drawdown = net_value[index_end] - net_value[index_start]
        # 最大回撤时间段
        max_drawdown_start = self.dt[index_start]
        max_drawdown_end = self.dt[index_end]

        return annual_rtn, annual_std, sharpe, max_drawdown, max_drawdown_start, max_drawdown_end

    def calcIndicatorByYear(self, net_value):

        # 分年度进行统计，这里需要注意，标准差的计算是使用的无偏

        nv = pd.DataFrame(net_value, index=self.dt, columns=['NV'])

        # ############ 总的统计结果 ################

        dd = nv - nv.expanding().max()
        mdd = dd.min().values[0]
        mdd_end = dd.idxmin().values[0]
        mdd_start = nv.loc[:mdd_end].idxmax().values[0]
        rtn_daily = nv.pct_change()
        total_df = pd.DataFrame({'AnnualRtn': rtn_daily.mean().values[0] * 250.,
                                 'AnnualVol': rtn_daily.std().values[0] * np.sqrt(250.),
                                 'Sharpe': rtn_daily.mean().values[0] * np.sqrt(250.) / rtn_daily.std().values[0],
                                 'MaxDrawdown': mdd,
                                 'MaxDDStart': mdd_start,
                                 'MaxDDEnd': mdd_end,
                                 'Days': nv.count().values[0],
                                 'NetValueInit': nv['NV'].iloc[0],
                                 'NetValueFinal': nv['NV'].iloc[-1]}, index=['total'])

        # ########### 按年度统计得到的结果 #####################

        rtn_daily['year'] = [i.year for i in rtn_daily.index]
        grouped = rtn_daily.groupby(by='year')
        rtn_mean = grouped.mean() * 250.
        rtn_std = grouped.std() * np.sqrt(250.)
        sharpe = rtn_mean / rtn_std

        nv['year'] = [i.year for i in nv.index]
        nv_grouped = nv.groupby(by='year')

        max_drawdown = nv_grouped.apply(func=lambda x: (x[['NV']] - x[['NV']].expanding().max()).min())
        max_drawdown_end = nv_grouped.apply(func=lambda x: (x[['NV']] - x[['NV']].expanding().max()).idxmin())
        max_drawdown_start = nv_grouped.apply(func=lambda x: x[['NV']].loc[:(x[['NV']] - x[['NV']].expanding().max()).
                                              idxmin().values[0]].idxmax())
        days = nv_grouped.count()
        nv_init = nv_grouped.apply(func=lambda x: x[['NV']].iloc[0])
        nv_final = nv_grouped.apply(func=lambda x: x[['NV']].iloc[-1])

        res_df = pd.DataFrame()

        rtn_mean.rename(columns={'NV': 'AnnualRtn'}, inplace=True)
        rtn_std.rename(columns={'NV': 'AnnualVol'}, inplace=True)
        sharpe.rename(columns={'NV': 'Sharpe'}, inplace=True)
        max_drawdown.rename(columns={'NV': 'MaxDrawdown'}, inplace=True)
        max_drawdown_start.rename(columns={'NV': 'MaxDDStart'}, inplace=True)
        max_drawdown_end.rename(columns={'NV': 'MaxDDEnd'}, inplace=True)
        days.rename(columns={'NV': 'Days'}, inplace=True)
        nv_init.rename(columns={'NV': 'NetValueInit'}, inplace=True)
        nv_final.rename(columns={'NV': 'NetValueFinal'}, inplace=True)

        res_df = res_df.join(rtn_mean, how='outer')
        res_df = res_df.join(rtn_std, how='outer')
        res_df = res_df.join(sharpe, how='outer')
        res_df = res_df.join(max_drawdown, how='outer')
        res_df = res_df.join(max_drawdown_start, how='outer')
        res_df = res_df.join(max_drawdown_end, how='outer')
        res_df = res_df.join(days, how='outer')
        res_df = res_df.join(nv_init, how='outer')
        res_df = res_df.join(nv_final, how='outer')
        res_df = pd.concat((res_df, total_df), sort=False)

        pd.set_option('expand_frame_repr', True)
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', 200)

        print(res_df)

        return res_df





if __name__ == '__main__':

    ttest1 = TradeRecordByTimes()
    ttest1.setDT('20181203')
    ttest1.setContract('TA1901.CZC')
    ttest1.setPrice(6000)
    ttest1.setVolume(10)
    ttest1.setDirection(1)
    ttest1.setType(1)
    ttest1.setMultiplier(5)
    ttest1.setMarginRatio(0.1)
    ttest1.calMarginOccupation()

    # tdtest = TradeRecordByDay(ttest1)

    print(ttest1)

    a1 = {'TA1901.CZC': {}}
    a2 = dict()
    a = TradeRecordByDay(dt='20181203', holdPosDict=a2, MkData=a2, newTrade=[])
    a.addNewPositon()
    print(a.getFinalMK())
    print(a.getHoldPosition())
    # a = BacktestSys()
    # print a.net_value
    # print a.calcSharpe()
    # print a.calcMaxDrawdown()
    # a.generateNV()
    # print a.calcBTResult()
    # plt.plot(a.net_value)
    # plt.show()
    # a.start_date = '20180101'
    # a.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC')
