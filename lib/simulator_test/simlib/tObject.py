"""
用于回测的交易的类
auther: YUANJIE
"""

import numpy as np
from datetime import timedelta, datetime
import pymongo
import pandas as pd
from pprint import pprint


class TradeRecordByTimes(object):
    # 单次的交易记录
    def __init__(self):
        self.tt = None  # 交易时间
        self.dt = None  # 交易日期，根据self.tt直接算出
        self.trade_commodity = None  # 交易商品
        self.trade_contract = None  # 交易合约
        self.trade_direction = None  # 交易方向, 1为做多，-1为做空
        self.trade_price = None  # 交易价格
        self.trade_exchangeRate = None  # 交易时的汇率
        self.trade_volume = None  # 交易量
        self.trade_amount = None  # 交易额
        self.trade_multiplier = None  # 交易乘数
        self.trade_margin_ratio = None  # 保证金比率
        self.trade_margin_occupation = None  # 保证金占用
        self.trade_type = None  # 交易类型，是平仓还是开仓。1为开仓，-1为平仓
        self.trade_commodity_value = None  # 合约价值，一定是正值
        self.trade_cost_mode = None  # 手续费收取方式：percentage还是fixed
        self.trade_cost_unit = None  # 手续费
        self.trade_cost = 0.

    def setTT(self, val):
        self.tt = val
        self.dt = self.tt.replace(hour=0, minute=0, second=0, microsecond=0)


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
        return self.trade_margin_occupation

    def calValue(self):
        self.trade_commodity_value = self.trade_price * self.trade_multiplier * abs(self.trade_volume) * \
                                     self.trade_exchangeRate
        return self.trade_commodity_value


    def calCost(self):
        if self.trade_cost_mode == 'percentage':
            self.trade_cost = self.trade_price * self.trade_volume * self.trade_multiplier * self.trade_cost_unit * \
                              self.trade_exchangeRate
        elif self.trade_cost_mode == 'fixed':
            self.trade_cost = self.trade_volume * self.trade_cost_unit


class TradeRecordByTrade(object):
    # 逐笔的交易记录
    def __init__(self):
        self.open = np.nan
        self.open_tt = None
        self.close = np.nan
        self.close_tt = None
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
        self.holding_period = (self.close_tt - self.open_tt + timedelta(1)).days

    def calcTcost(self):
        if self.tcost_mode == 'percentage':
            self.tcost = (self.open + self.close) * self.volume * self.multiplier * self.tcost_unit
        elif self.tcost_mode == 'fixed':
            self.tcost = self.volume * 2. * self.tcost_unit

    def setOpen(self, val):
        self.open = val

    def setOpenTT(self, val):
        self.open_tt = val

    def setClose(self, val):
        self.close = val

    def setCloseTT(self, val):
        self.close_tt = val

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


class TradeRecordByDay(object):
    # 逐日的交易记录
    def __init__(self, dt, holdPosDict, MkData, newTrade):
        self.dt = dt  # 日期
        self.newTrade = newTrade  # 当天进行的交易
        # 合约市场数据
        # 结构是{"合约": {'CLOSE': ***, 'PRECLOSE': ***, 'ExRate': ***, 'multiplier': **, 'margin_ratio': **}}
        self.mkdata = MkData
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
            print(k, v)
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



# class TradeRecordIntraday(object):
#     # 用来记录逐日交易，计算当日的pnl
#     def __init__(self, dt, ystPosDict, holdPosIntraday,  mkData):
#         self.dt = dt  # 日期
#         self.ystPost = ystPosDict  # 前一交易日的持仓
#         self.holdPost = holdPosIntraday  # 该交易日当天的按照分钟级别的持仓情况
#         self.mkdata = mkData  # 该交易日当天的价格信息，包括收盘价、开盘价





if __name__ == '__main__':

    t1 = TradeRecordByTimes()
    t1.setTT(datetime(2020, 2, 25, 9, 50, 0))
    t1.setContract('RB2005.SHF')
    t1.setCommodity('RB.SHF')
    t1.setPrice(3446)
    t1.setVolume(20)
    t1.setDirection(-1)
    t1.setMultiplier(10)
    t1.setMarginRatio(0.07)
    t1.setType(1)
    t1.setExchangRate(1)
    t1.setCost(mode='percentage', value=0.00002)

    t2 = TradeRecordByTimes()
    t2.setTT(datetime(2020, 2, 25, 11, 20, 0))
    t2.setContract('RB2005.SHF')
    t2.setCommodity('RB.SHF')
    t2.setPrice(3480)
    t2.setVolume(20)
    t2.setDirection(1)
    t2.setMultiplier(10)
    t2.setMarginRatio(0.07)
    t2.setType(-1)
    t2.setExchangRate(1)
    t2.setCost(mode='percentage', value=0.00002)

    mkdata = {'RB2005.SHF': {'CLOSE': 3420, 'ExRate': 1, 'PRECLOSE': 3500, 'multiplier': 10, 'margin_ratio': 0.07}}
    holdingsDict = {'RU2005.SHF': {'volume': 10},
                    'I2005.DCE': {'volume': -5}}
    tdobj = TradeRecordByDay(dt=datetime(2020, 2, 25), holdPosDict=holdingsDict, MkData=mkdata, newTrade=[t1, t2])
    print(tdobj.getHoldPosition())
    tdobj.addNewPositon()
    tdobj.getFinalMK()
    print(tdobj.daily_pnl)
    print(tdobj.getHoldPosition())




    # conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
    # database = conn['CBNB']
    # database.authenticate(name='yuanjie', password='yuanjie')
    # minute_coll = database['FuturesMinMD']
    # queryArgs = {'wind_code': 'TA.CZC', 'date_time': {'$gte': datetime(2019, 7, 31, 21, 0, 0),
    #                                                   '$lte': datetime(2019, 8, 1, 15, 1, 0)}}
    # projectionFields = ['date_time', 'close', 'open']
    # records = minute_coll.find(queryArgs, projectionFields).sort('date_time', pymongo.ASCENDING)
    # df = pd.DataFrame.from_records(records)
    # df.drop(columns='_id', inplace=True)
    # df.dropna(how='all', subset=['open', 'close'], inplace=True)
    # res_dict = df.to_dict(orient='list')
    #
    # mkdata = {'TA.CZC': {'ExRate': 1,
    #                      'PRECLOSE': res_dict['open'][0],
    #                      'tm': res_dict['date_time'],
    #                      'OPEN': res_dict['open'],
    #                      'CLOSE': res_dict['close']}}
    #
    # holdings = np.ones(len(res_dict['open']))
    # holdings[50:100] = 0
    # holdings[150:200] = 2
    # print(holdings)
    # holdings = {'TA.CZC': {'tm': res_dict['date_time'],
    #                        'holdings': holdings}}
    # ystHoldings = {'TA.CZC': -1}
    #
    # print(holdings)
    #
    #
    #
    # test = TradeRecordIntraday(dt=datetime(2019, 8, 1), ystPosDict=ystHoldings, holdPosIntraday=holdings,
    #                            mkData=mkdata)



