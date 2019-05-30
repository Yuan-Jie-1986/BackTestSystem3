
import sys
import pandas as pd
import numpy as np
import pymongo
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pprint
import yaml
import os
from pandas.plotting import register_matplotlib_converters


# 需要导入的数据构造类
class DataClass(object):
    def __init__(self, nm):
        self.nm = nm
        self.ts_data_field = []  # 用来存储时间序列数据的变量名
        self.ts_string_field = []  # 用来存储时间序列的字符的变量名

    def add_dt(self, dt):
        self.dt = np.array(dt)

    # 检查时间长度与时间序列的变量长度是否一致
    def check_len(self):
        dt_len = len(self.dt)
        ts_field = self.ts_data_field + self.ts_string_field
        for field in ts_field:
            temp = getattr(self, field)
            if len(temp) != dt_len:
                raise Exception('%s的%s与时间的长度不一致' % (self.nm, field))

    # 自动检查长度是否一致的装饰器
    def auto_check(func):
        def inner(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.check_len()
            return
        return inner

    # 添加时间序列的变量
    @auto_check
    def add_ts_data(self, obj_name, obj_value):
        self.ts_data_field.append(obj_name)
        obj_value = np.array(obj_value)
        setattr(self, obj_name, obj_value)

    # 更新时间序列的变量
    @auto_check
    def update_ts_data(self, obj_name, obj_value):
        if obj_name not in self.ts_data_field:
            raise Exception('%s不在时间序列数据名列表中，请检查或使用add_ts_data函数' % obj_name)
        obj_value = np.array(obj_value)
        setattr(self, obj_name, obj_value)

    # 添加时间序列变量，但是字符串类型
    @auto_check
    def add_ts_string(self, obj_name, obj_value):
        self.ts_string_field.append(obj_name)
        obj_value = np.array(obj_value)
        setattr(self, obj_name, obj_value)

    # 更新字符口串类型的时间序列变量
    @auto_check
    def update_ts_string(self, obj_name, obj_value):
        if obj_name not in self.ts_string_field:
            raise Exception('%s不在字符串类型的时间序列数据名列表中，请检查或使用add_ts_string函数' % obj_name)
        obj_value = np.array(obj_value)
        setattr(self, obj_name, obj_value)


    # 添加变量，主要是起到标识的作用
    def add_data(self, obj_name, obj_value):
        setattr(self, obj_name, obj_value)

    # 对于时间序列数据进行填充
    @auto_check
    def fillna_ts_data(self, obj_name, method='ffill'):
        if obj_name not in self.ts_data_field and obj_name not in self.ts_string_field:
            raise Exception('%s不在时间序列数据列表' % obj_name)
        temp = getattr(self, obj_name)
        temp = pd.DataFrame(temp).fillna(method=method).values.flatten()
        setattr(self, obj_name, temp)


    # 根据新的时间序列重新生成时间序列变量
    @auto_check
    def rearrange_ts_data(self, dt_new):
        con1 = np.in1d(self.dt, dt_new)
        con2 = np.in1d(dt_new, self.dt)
        for field in self.ts_data_field:
            temp = np.ones(len(dt_new)) * np.nan
            temp[con2] = getattr(self, field)[con1]
            setattr(self, field, temp)
        for field in self.ts_string_field:
            temp = np.array(len(dt_new) * [None])
            temp[con2] = getattr(self, field)[con1]
            setattr(self, field, temp)
        self.dt = dt_new


# 持仓数据类
class HoldingClass(object):
    def __init__(self, dt):
        # self.asset存储所持有的合约名
        # self.合约名是持仓数据
        self.dt = dt
        self.asset = []
        self.newest_holdings = {}

    # 检查时间长度与持仓变量长度是否一致
    def check_len(self):
        dt_len = len(self.dt)
        for field in self.asset:
            temp = getattr(self, field)
            if len(temp) != dt_len:
                raise Exception('%s的持仓数据与时间的长度不一致，%s的长度是%d, 时间的长度是%d' %
                                (field, field, len(temp), dt_len))

    # 自动检查长度是否一致的装饰器
    def auto_check(func):
        def inner(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.check_len()
            return
        return inner

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
    def shift_holdings(self):
        for h in self.asset:
            temp = getattr(self, h)
            new_holdings = np.zeros(len(self.dt))
            new_holdings[1:] = temp[:-1]
            setattr(self, h, new_holdings)

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
        for i in range(1, len(self.dt)):
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
        holding_df.index = self.dt
        return holding_df



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
        self.trade_amount = None  # 交易额
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
        return self.trade_margin_occupation

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
        self.conf = yaml.load(f, Loader=yaml.FullLoader)

        # 回测起始时间
        self.start_dt = datetime.strptime(self.conf['start_date'], '%Y%m%d')

        # 回测结束时间
        self.end_dt = datetime.strptime(self.conf['end_date'], '%Y%m%d')

        # 初始资金
        self.capital = np.float(self.conf['capital'])

        # 数据库的配置信息
        host = self.conf['host']
        port = self.conf['port']
        usr = self.conf['user']
        pwd = self.conf['pwd']
        db_nm = self.conf['db_name']

        conn = pymongo.MongoClient(host=host, port=port)
        self.db = conn[db_nm]
        self.db.authenticate(name=usr, password=pwd)

        # 将需要的数据信息存到字典self.data中，key是yaml中配置的数据分类，比如futures_price，value仍然是个字典，{品种:数据实例}
        # e.g. self.data = {'future_price': {'TA.CZC': DataClass的实例}}

        raw_data = self.conf['data']
        self.data = {}
        self.unit = self.conf['trade_unit']
        self.bt_mode = self.conf['backtest_mode']
        self.margin_ratio = self.conf['margin_ratio']
        self.tcost = self.conf['tcost']
        self.switch_contract = self.conf['switch_contract']
        self.turnover = self.conf['turnover']
        self.exchange_func = {}

        if self.tcost:
            self.tcost_list = self.conf['tcost_list']
        exchange_list = []
        for sub_class, sub_data in raw_data.items():
            self.data[sub_class] = {}
            for d in sub_data:
                self.data[sub_class][d['obj_content']] = DataClass(nm=d['obj_content'])
                table = self.db[d['collection']] if 'collection' in d else self.db['FuturesMD']
                query_arg = {d['obj_field']: d['obj_content'], 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
                projection_fields = ['date'] + d['fields']
                if self.switch_contract:
                    projection_fields = projection_fields + ['switch_contract', 'specific_contract']
                res = table.find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
                df_res = pd.DataFrame.from_records(res)
                df_res.drop(columns='_id', inplace=True)
                dict_res = df_res.to_dict(orient='list')
                self.data[sub_class][d['obj_content']].add_dt(dict_res['date'])
                for k, v in dict_res.items():
                    if k != 'date' and k != 'specific_contract':
                        self.data[sub_class][d['obj_content']].add_ts_data(k, v)
                    elif k == 'specific_contract':
                        self.data[sub_class][d['obj_content']].add_ts_string(k, v)
                self.data[sub_class][d['obj_content']].add_data('commodity', d['commodity'])
                self.data[sub_class][d['obj_content']].add_data('unit_change',
                                                                d['unit_change'] if 'unit_change' in d else 'unchange')
                self.data[sub_class][d['obj_content']].add_data('frequency',
                                                                d['frequency'] if 'frequency' in d else 'daily')

                exchange_list.append(self.data[sub_class][d['obj_content']].unit_change)

        # 需要导入美元兑人民币数据
        if 'dollar' in exchange_list:
            self.exchange_func['dollar'] = 'dollar2rmb'
            query_arg = {'wind_code': 'M0067855', 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date', 'CLOSE']
            res = self.db['EDB'].find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            dict_res = df_res.to_dict(orient='list')
            self.dollar2rmb = DataClass(nm=self.exchange_func['dollar'])
            self.dollar2rmb.add_dt(dict_res['date'])
            for k, v in dict_res.items():
                if k != 'date':
                    self.dollar2rmb.add_ts_data(k, v)

        # 将提取的数据按照交易时间的并集重新生成
        date_set = set()
        for sub_class, sub_data in self.data.items():
            for d in sub_data:
                date_set = date_set.union(sub_data[d].dt)
        self.dt = np.array(list(date_set))
        self.dt.sort()

        # 针对周频数据转频到日频
        for sub_class, sub_data in self.data.items():
            for d in sub_data:
                if sub_data[d].frequency == 'weekly':
                    dt_weekly = sub_data[d].dt
                    self.data[sub_class][d].add_dt(self.dt)
                    for f in sub_data[d].ts_data_field:
                        temp_df = pd.DataFrame(getattr(sub_data[d], f), index=dt_weekly, columns=[f])
                        temp_df = temp_df.reindex(self.dt)
                        temp_df.fillna(method='ffill', inplace=True)
                        self.data[sub_class][d].update_ts_data(f, temp_df.values.flatten())


        # 如果定义了date_type，则去调取交易日期序列
        if 'date_type' in self.conf:
            self.date_type = self.conf['date_type']
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
        for sub_class in self.data:
            for d in self.data[sub_class]:
                self.data[sub_class][d].rearrange_ts_data(self.dt)

        # 根据交易日期序列重新整理汇率，并且先向后填充，然后再向前填充
        for k in self.exchange_func:
            getattr(self, self.exchange_func[k]).rearrange_ts_data(self.dt)
            getattr(self, self.exchange_func[k]).fillna_ts_data(obj_name='CLOSE', method='ffill')
            if np.isnan(getattr(self, self.exchange_func[k]).CLOSE).any():
                print('%s出现了nan值，使用向前填充' % self.exchange_func[k])
                print(self.dt[np.isnan(getattr(self, self.exchange_func[k]).CLOSE)])
                getattr(self, self.exchange_func[k]).fillna_ts_data(obj_name='CLOSE', method='bfill')

        # 在exchange_func中增加与unchange对应的函数
        if 'unchange' in exchange_list:
            self.exchange_func['unchange'] = 'unchange'
            self.unchange = DataClass(nm=self.exchange_func['unchange'])
            self.unchange.add_dt(self.dt)
            self.unchange.add_ts_data('CLOSE', np.ones(len(self.dt)))


    def strategy(self):
        raise NotImplementedError

    def holdingsProcess(self, holdingsObj):
        # 对生成的持仓进行处理，需要注意的是这个函数要最后再用
        # 如果在配置文件中的持仓周期大于1，需要对持仓进行调整。通常该参数是针对alpha策略。
        if self.turnover > 1:
            print('根据turnover对持仓进行调整，请检查是否为alpha策略')
            holdingsObj.adjust_holdings_turnover(self.turnover)

        if self.bt_mode == 'OPEN':
            # 如果是开盘价进行交易，则将初始持仓向后平移一位
            holdingsObj.shift_holdings()


        # 如果当天合约没有交易量，那么需要对权重进行调整
        for h in holdingsObj.asset:
            h_holdings = getattr(holdingsObj, h)
            h_cls = self.data['future_price'][h].CLOSE
            if 'OPEN' in self.data['future_price'][h].__dict__:
                h_opn = self.data['future_price'][h].OPEN
            for i in range(1, len(self.dt)):
                if np.isnan(h_cls[:i+1]).all():
                    continue
                if (np.isnan(h_cls[i]) or ('OPEN' in self.data['future_price'][h].__dict__ and np.isnan(h_opn[i]))) and \
                    h_holdings[i] != h_holdings[i-1]:
                    print('%s合约在%s这一天没有成交，对持仓进行了调整，调整前是%f，调整后是%f' % \
                          (h, self.dt[i].strftime('%Y%m%d'), h_holdings[i], h_holdings[i - 1]))
                    h_holdings[i] = h_holdings[i-1]
                    # setattr(holdingsObj, h, h_holdings)  这一句不用，因为取出的list内存地址是共用的。
        return holdingsObj

    def holdingsStandardization(self, holdingsObj, mode=0):
        """根据给定的持仓重新生成标准化的持仓：
        mode=0：不加杠杆。所有持仓品种的合约价值相同。每天的持仓情况会根据每日的收盘价而发生变化。这样会产生一些不必要的交易。
                价格越低，越增加持仓，价格越高，越减少持仓。
        mode=1: 不加杠杆。所有持仓品种的合约价值相同。如果某个品种的初始持仓没有变化，那就不调整。某个品种持仓有变化，就根据剩余
                资金按照合约价值来分配持仓。
        mode=2: 不加杠杆。所有持仓品种按照其波动率进行调整，按照相对的波动来对持仓进行分配。
        mode=3: 不加杠杆。所有持仓品种按照ATR进行调整，按照ATR来对持仓进行分配。计算ATR时需要最高价最低价
        """
        # 合约持仓的dataframe
        holdings_df = holdingsObj.to_frame()

        # 计算出各合约做1手的合约价值
        cls_df = pd.DataFrame()
        for k, v in self.data['future_price'].items():
            cls_temp = v.CLOSE * self.unit[v.commodity]
            unit_change = getattr(self, self.exchange_func[v.unit_change]).CLOSE
            cls_df[k] = cls_temp * unit_change
        cls_df.index = self.dt

        if mode == 0 or mode == 1:

            # 根据持仓得到每日的持有几个合约，针对每个合约平均分配资金
            holdings_num = np.abs(np.sign(holdings_df))
            holdings_num = holdings_num.sum(axis=1)
            holdings_num[holdings_num == 0] = np.nan
            sub_capital = self.capital / holdings_num

            cls_df = cls_df * np.sign(holdings_df)
            cls_df[cls_df == 0] = np.nan

            holdings_new = pd.DataFrame()
            for c in cls_df:
                holdings_new[c] = sub_capital / cls_df[c]

            holdings_new.fillna(0, inplace=True)

            if mode == 0:
                holdings_new = holdings_new.round(decimals=0)
                for h in holdingsObj.asset:
                    holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
                return holdingsObj

            elif mode == 1:
                # 判断初始持仓是否与前一天的初始持仓相同
                holdings_yestd = holdings_df.shift(periods=1)
                holdings_equal = holdings_df == holdings_yestd

                # 统计当天持仓的合约个数
                holdings_temp = holdings_df.copy(deep=True)
                holdings_temp[holdings_temp == 0] = np.nan
                holdings_num = holdings_temp.count(axis=1)
                holdings_num_yestd = holdings_num.shift(periods=1)

                # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
                num_equal = holdings_num == holdings_num_yestd
                holdings_equal.loc[~num_equal] = False
                holdings_new[holdings_equal] = np.nan
                holdings_new.fillna(method='ffill', inplace=True)
                holdings_new = holdings_new.round(decimals=0)
                for h in holdingsObj.asset:
                    holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
                return holdingsObj


        elif mode == 2:
            # 计算各合约过去一年的合约价值的标准差
            std_df = cls_df.rolling(window=250, min_periods=200).std()
            # 根据合约价值的标准差的倒数的比例来分配资金
            ratio_df = self.capital / std_df
            ratio_df[holdings_df == 0] = np.nan
            ratio_total = ratio_df.sum(axis=1)
            sub_capital = pd.DataFrame()
            for c in ratio_df:
                sub_capital[c] = ratio_df[c] / ratio_total * self.capital

            # 根据分配资金进行权重的重新生成
            holdings_new = sub_capital / cls_df * np.sign(holdings_df)
            holdings_new.fillna(0, inplace=True)

            # 如果初始权重不变，则不对权重进行调整
            # 判断初始持仓是否与前一天的初始持仓相同
            holdings_yestd = holdings_df.shift(periods=1)
            holdings_equal = holdings_df == holdings_yestd

            # 统计当天持仓的合约个数
            holdings_temp = holdings_df.copy(deep=True)
            holdings_temp[holdings_temp == 0] = np.nan
            holdings_num = holdings_temp.count(axis=1)
            holdings_num_yestd = holdings_num.shift(periods=1)

            # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
            num_equal = holdings_num == holdings_num_yestd
            holdings_equal.loc[~num_equal] = False
            holdings_new[holdings_equal] = np.nan
            holdings_new.fillna(method='ffill', inplace=True)
            holdings_new = holdings_new.round(decimals=0)

            for h in holdingsObj.asset:
                holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
            return holdingsObj

        elif mode == 3:
            # 使用ATR，需要最高价最低价，否则会报错

            cls_atr_df = pd.DataFrame()
            high_atr_df = pd.DataFrame()
            low_atr_df = pd.DataFrame()
            for k, v in self.data['future_price'].items():
                cls_atr = v.CLOSE * self.unit[v.commodity]
                high_atr = v.HIGH * self.unit[v.commodity]
                low_atr = v.LOW * self.unit[v.commodity]
                unit_change = getattr(self, self.exchange_func[v.unit_change]).CLOSE
                cls_atr_df[k] = cls_atr * unit_change
                high_atr_df[k] = high_atr * unit_change
                low_atr_df[k] = low_atr * unit_change
            cls_atr_df.index = self.dt
            high_atr_df.index = self.dt
            low_atr_df.index = self.dt

            cls_atr_yestd_df = cls_atr_df.shift(periods=1)

            p1 = high_atr_df - low_atr_df
            p2 = np.abs(high_atr_df - cls_atr_yestd_df)
            p3 = np.abs(cls_atr_yestd_df - low_atr_df)

            true_range = np.maximum(p1, np.maximum(p2, p3))
            atr = pd.DataFrame(true_range).rolling(window=250, min_periods=200).mean()

            # 根据合约价值的ATR的倒数的比例来分配资金
            ratio_df = self.capital / atr
            ratio_df[holdings_df == 0] = np.nan
            ratio_total = ratio_df.sum(axis=1)
            sub_capital = pd.DataFrame()
            for c in ratio_df:
                sub_capital[c] = ratio_df[c] / ratio_total * self.capital

            # 根据分配资金进行权重的重新生成
            holdings_new = sub_capital / cls_df * np.sign(holdings_df)
            holdings_new.fillna(0, inplace=True)

            # 如果初始权重不变，则不对权重进行调整
            # 判断初始持仓是否与前一天的初始持仓相同
            holdings_yestd = holdings_df.shift(periods=1)
            holdings_equal = holdings_df == holdings_yestd

            # 统计当天持仓的合约个数
            holdings_temp = holdings_df.copy()
            holdings_temp[holdings_temp == 0] = np.nan
            holdings_num = holdings_temp.count(axis=1)
            holdings_num_yestd = holdings_num.shift(periods=1)

            # 统计当天持仓个数是否与前一天持仓个数相同, 如果num_equal是True，那么持仓个数与前一天的相同
            num_equal = holdings_num == holdings_num_yestd
            holdings_equal.loc[~num_equal] = False
            holdings_new[holdings_equal] = np.nan
            holdings_new.fillna(method='ffill', inplace=True)
            holdings_new = holdings_new.round(decimals=0)

            for h in holdingsObj.asset:
                holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
            return holdingsObj


    def getPnlDaily(self, holdingsObj):
        '''
        根据持仓情况计算每日的pnl，每日的保证金占用，每日的合约价值
        holdingsObj是持仓数据类的实例
        根据持仓情况统计每日换手率，即成交额/总资金，应该是在0-2之间
        需要注意的一点是，如果换合约的时候，没有成交量，使用的是之前的价格进行的平仓，导致的回测的pnl和换手不准确，暂时无法解决
        '''

        pnl_daily = np.zeros_like(self.dt).astype('float')
        margin_occ_daily = np.zeros_like(self.dt).astype('float')
        value_daily = np.zeros_like(self.dt).astype('float')
        turnover_daily = np.zeros_like(self.dt).astype('float')

        holdpos = {}

        future_price = self.data['future_price']

        for i, v in enumerate(self.dt):
            newtradedaily = []
            mkdata = {}
            for h in holdingsObj.asset:

                cls_td = getattr(future_price[h], 'CLOSE')[i]
                exrate_td = getattr(self, self.exchange_func[future_price[h].unit_change]).CLOSE[i]
                holdings_td = getattr(holdingsObj, h)[i]

                # 如果在当前日期该合约没有开始交易，则直接跳出当前循环，进入下一个合约
                if np.isnan(future_price[h].CLOSE[:i+1]).all():
                    continue
                if np.isnan(cls_td):
                    print('%s合约在%s这一天没有收盘数据' % (h, v.strftime('%Y%m%d')))
                    continue
                # 需要传入的市场数据
                mkdata[h] = {'CLOSE': cls_td,
                             'ExRate': exrate_td ,
                             'multiplier': self.unit[future_price[h].commodity],
                             'margin_ratio': self.margin_ratio[h]}

                # 合约首日交易便有持仓时
                if i == 0 or np.isnan(future_price[h].CLOSE[:i]).all():
                    if holdings_td != 0:
                        newtrade = TradeRecordByTimes()
                        newtrade.setDT(v)
                        newtrade.setContract(h)
                        newtrade.setCommodity(future_price[h].commodity)
                        newtrade.setPrice(getattr(future_price[h], self.bt_mode)[i])
                        newtrade.setExchangRate(exrate_td)
                        newtrade.setType(1)
                        newtrade.setVolume(abs(holdings_td))
                        newtrade.setMarginRatio(self.margin_ratio[h])
                        newtrade.setMultiplier(self.unit[future_price[h].commodity])
                        newtrade.setDirection(np.sign(holdings_td))
                        if self.tcost:
                            newtrade.setCost(**self.tcost_list[h])
                        newtrade.calCost()
                        turnover_daily[i] += newtrade.calMarginOccupation()
                        newtradedaily.append(newtrade)

                # 如果不是第一天交易的话，需要前一天的收盘价
                elif i != 0:
                    holdings_ystd = getattr(holdingsObj, h)[i - 1]
                    mkdata[h]['PRECLOSE'] = future_price[h].CLOSE[i - 1]
                    mkdata[h]['PRECLOSE_ExRate'] = getattr(self, self.exchange_func[future_price[h].unit_change]).CLOSE[i - 1]
                    if np.isnan(mkdata[h]['PRECLOSE']):
                        for pre_counter in np.arange(2, i + 1):
                            if ~np.isnan(future_price[h].CLOSE[i - pre_counter]):
                                mkdata[h]['PRECLOSE'] = future_price[h].CLOSE[i - pre_counter]
                                mkdata[h]['PRECLOSE_ExRate'] = getattr(
                                    self, self.exchange_func[future_price[h].unit_change]).CLOSE[i - pre_counter]
                                print('%s合约在%s使用的PRECLOSE是%d天前的收盘价' % (h, self.dt[i].strftime('%Y%m%d'), pre_counter))
                                break

                    # 如果切换主力合约
                    if self.switch_contract:
                        mkdata[h].update({'switch_contract': future_price[h].switch_contract[i],
                                          'specific_contract': future_price[h].specific_contract[i - 1]})
                        # 如果switch_contract为True，需要前主力合约的OPEN
                        if mkdata[h]['switch_contract'] and ~np.isnan(mkdata[h]['switch_contract']):
                            # 这里需要注意的是np.nan也会判断为true，所以需要去掉
                            # 比如MA.CZC在刚开始的时候是没有specific_contract
                            # if np.isnan(mkdata[h]['switch_contract']):
                            #     raise Exception('adsfasdfasdfsafsa')
                            queryArgs = {'wind_code': mkdata[h]['specific_contract'], 'date': self.dt[i]}
                            projectionField = ['OPEN']
                            table = self.db['FuturesMD']
                            if mkdata[h]['specific_contract'] == 'nan':
                                # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                                print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                                      (h, self.dt[i].strftime('%Y%m%d')))
                                old_open = future_price[h].CLOSE[i - 1]
                                old_open_exrate = getattr(self, self.exchange_func[future_price[h].unit_change]).CLOSE[i - 1]
                            elif table.find_one(queryArgs, projectionField):
                                old_open = table.find_one(queryArgs, projectionField)['OPEN']
                                old_open_exrate = getattr(self, self.exchange_func[future_price[h].unit_change]).CLOSE[i]
                                if np.isnan(old_open):
                                    print('%s因为该合约当天没有交易，在%s使用前一天的收盘价作为换约平仓的价格' % \
                                          (mkdata[h]['specific_contract'], self.dt[i].strftime('%Y%m%d')))
                                    # 这样的处理方法有个问题，在实盘的交易中无法实现这样的操作。这里只是回测时的处理方法
                                    old_open = mkdata[h]['PRECLOSE']
                                    old_open_exrate = mkdata[h]['PRECLOSE_ExRate']
                            else:
                                print('%s因为已经到期，在%s使用的是前一天的收盘价作为换约平仓的价格' % \
                                      (mkdata[h]['specific_contract'], self.dt[i].strftime('%Y%m%d')))
                                old_open = mkdata[h]['PRECLOSE']
                                old_open_exrate = mkdata[h]['PRECLOSE_ExRate']

                    if self.switch_contract and mkdata[h]['switch_contract'] and ~np.isnan(mkdata[h]['switch_contract'])\
                            and holdings_ystd != 0:
                        newtrade1 = TradeRecordByTimes()
                        newtrade1.setDT(v)
                        newtrade1.setContract(h)
                        newtrade1.setCommodity(future_price[h].commodity)
                        newtrade1.setPrice(old_open)
                        newtrade1.setExchangRate(old_open_exrate)
                        newtrade1.setVolume(abs(holdings_ystd))
                        newtrade1.setMarginRatio(self.margin_ratio[h])
                        newtrade1.setMultiplier(self.unit[future_price[h].commodity])
                        newtrade1.setDirection(-np.sign(holdings_ystd))
                        if self.tcost:
                            newtrade1.setCost(**self.tcost_list[h])
                        newtrade1.calCost()
                        turnover_daily[i] += newtrade1.calMarginOccupation()
                        newtradedaily.append(newtrade1)

                        if holdings_td != 0:
                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setDT(v)
                            newtrade2.setContract(h)
                            newtrade2.setCommodity(future_price[h].commodity)
                            newtrade2.setPrice(getattr(future_price[h], self.bt_mode)[i])
                            newtrade2.setExchangRate(exrate_td)
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(holdings_td))
                            newtrade2.setMarginRatio(self.margin_ratio[h])
                            newtrade2.setMultiplier(self.unit[future_price[h].commodity])
                            newtrade2.setDirection(np.sign(holdings_td))
                            if self.tcost:
                                newtrade2.setCost(**self.tcost_list[h])
                            newtrade2.calCost()
                            turnover_daily[i] += newtrade2.calMarginOccupation()
                            newtradedaily.append(newtrade2)

                    else:
                        if holdings_td * holdings_ystd < 0:
                            newtrade1 = TradeRecordByTimes()
                            newtrade1.setDT(v)
                            newtrade1.setContract(h)
                            newtrade1.setCommodity(future_price[h].commodity)
                            newtrade1.setPrice(getattr(future_price[h], self.bt_mode)[i])
                            newtrade1.setExchangRate(exrate_td)
                            newtrade1.setType(-1)
                            newtrade1.setVolume(abs(holdings_ystd))
                            newtrade1.setMarginRatio(self.margin_ratio[h])
                            newtrade1.setMultiplier(self.unit[future_price[h].commodity])
                            newtrade1.setDirection(np.sign(holdings_td))
                            if self.tcost:
                                newtrade1.setCost(**self.tcost_list[h])
                            newtrade1.calCost()
                            turnover_daily[i] += newtrade1.calMarginOccupation()
                            newtradedaily.append(newtrade1)

                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setDT(v)
                            newtrade2.setContract(h)
                            newtrade2.setCommodity(future_price[h].commodity)
                            newtrade2.setPrice(getattr(future_price[h], self.bt_mode)[i])
                            newtrade2.setExchangRate(exrate_td)
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(holdings_td))
                            newtrade2.setMarginRatio(self.margin_ratio[h])
                            newtrade2.setMultiplier(self.unit[future_price[h].commodity])
                            newtrade2.setDirection(np.sign(holdings_td))
                            if self.tcost:
                                newtrade2.setCost(**self.tcost_list[h])
                            newtrade2.calCost()
                            turnover_daily[i] += newtrade2.calMarginOccupation()
                            newtradedaily.append(newtrade2)

                        elif holdings_td == holdings_ystd:  # 没有交易
                            continue

                        else:
                            newtrade = TradeRecordByTimes()
                            newtrade.setDT(v)
                            newtrade.setContract(h)
                            newtrade.setCommodity(future_price[h].commodity)
                            newtrade.setPrice(getattr(future_price[h], self.bt_mode)[i])
                            newtrade.setExchangRate(exrate_td)
                            newtrade.setType(np.sign(abs(holdings_td) - abs(holdings_ystd)))
                            newtrade.setVolume(abs(holdings_td - holdings_ystd))
                            newtrade.setMarginRatio(self.margin_ratio[h])
                            newtrade.setMultiplier(self.unit[future_price[h].commodity])
                            newtrade.setDirection(np.sign(holdings_td - holdings_ystd))
                            if self.tcost:
                                newtrade.setCost(**self.tcost_list[h])
                            newtrade.calCost()
                            turnover_daily[i] += newtrade.calMarginOccupation()
                            newtradedaily.append(newtrade)

            trd = TradeRecordByDay(dt=v, holdPosDict=holdpos, MkData=mkdata, newTrade=newtradedaily)
            trd.addNewPositon()
            pnl_daily[i], margin_occ_daily[i], value_daily[i] = trd.getFinalMK()
            holdpos = trd.getHoldPosition()
            turnover_daily[i] = turnover_daily[i] / self.capital

        return pnl_daily, margin_occ_daily, value_daily, turnover_daily

    def getNV(self, holdingsObj):
        # 计算总的资金曲线变化情况
        return self.capital + np.cumsum(self.getPnlDaily(holdingsObj)[0])

    def statTrade(self, holdingsObj):
        """
        对每笔交易进行统计

        """

        total_pnl = 0.
        trade_record = {}
        uncovered_record = {}

        future_price = self.data['future_price']
        for k in holdingsObj.asset:
            trade_record[k] = []
            uncovered_record[k] = []
            count = 0
            trade_price = getattr(future_price[k], self.bt_mode)
            holdings = getattr(holdingsObj, k)

            for i in range(len(holdings)):

                if i == 0 and holdings[i] == 0:
                    continue
                # if np.isnan(trade_price[i]):
                #     # 需要注意的是如果当天没有成交量，可能一些价格会是nan值，会导致回测计算结果不准确
                #     # 如果当天没有交易量的话，所持有的仓位修改成与单一个交易日相同
                #     v[i] = v[i-1]
                #     continue

                # 如果当天涉及到移仓，需要将昨天的仓位先平掉，然后在新的主力合约上开仓，统一以开盘价平掉旧的主力合约
                if self.switch_contract and future_price[k].switch_contract[i] \
                    and ~np.isnan(future_price[k].switch_contract[i]) and holdings[i - 1] != 0:

                    # 条件中需要加上判断合约是否为nan，否则也会进入到该条件中

                    table = self.db['FuturesMD']
                    res = future_price[k].specific_contract[i-1]
                    # 对于换合约需要平仓的合约均使用开盘价进行平仓
                    queryArgs = {'wind_code': res, 'date': self.dt[i]}
                    projectionField = ['OPEN']

                    if res == 'nan':
                        # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                        print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                              (k, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = future_price[k].CLOSE[i-1]
                        trade_exrate_switch = getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i-1]
                    elif table.find_one(queryArgs, projectionField):
                        trade_price_switch = table.find_one(queryArgs, projectionField)['OPEN']
                        trade_exrate_switch = getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i]
                        if np.isnan(trade_price_switch):
                            print('%s因为该合约当天没有交易，在%s使用前一天的收盘价作为换约平仓的价格' % \
                                  (res, self.dt[i].strftime('%Y%m%d')))
                            trade_price_switch = future_price[k].CLOSE[i-1]
                            trade_exrate_switch = getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i - 1]
                    else:
                        print('%s因为已经到期，在%s使用的是前一天的收盘价作为换约平仓的价格' % \
                              (res, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = future_price[k].CLOSE[i-1]
                        trade_exrate_switch = getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i - 1]

                    if uncovered_record[k]:
                        needed_covered = abs(holdings[i - 1])
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

                        if holdings[i] != 0:
                            # 对新的主力合约进行开仓
                            count += 1
                            tr_r = TradeRecordByTrade()
                            tr_r.setCounter(count)
                            tr_r.setOpen(trade_price[i])
                            tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
                            tr_r.setOpenDT(self.dt[i])
                            tr_r.setCommodity(future_price[k].commodity)
                            tr_r.setVolume(abs(holdings[i]))
                            tr_r.setMultiplier(self.unit[future_price[k].commodity])
                            tr_r.setContract(k)
                            tr_r.setDirection(np.sign(holdings[i]))
                            if self.tcost:
                                tr_r.setTcost(**self.tcost_list[k])
                            trade_record[k].append(tr_r)
                            uncovered_record[k].append(tr_r.count)

                else:
                    if (holdings[i] != 0 and i == 0) or (holdings[i] != 0 and np.isnan(trade_price[:i]).all()):
                        # 第一天交易就开仓
                        # 第二种情况是为了排除该品种当天没有交易，价格为nan的这种情况，e.g. 20141202 BU.SHF
                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(future_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]))
                        tr_r.setMultiplier(self.unit[future_price[k].commodity])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
                        if self.tcost:
                            tr_r.setTcost(**self.tcost_list[k])
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)

                    elif abs(holdings[i]) > abs(holdings[i-1]) and holdings[i] * holdings[i-1] >= 0:
                        # 新开仓或加仓

                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(future_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]) - abs(holdings[i-1]))
                        tr_r.setMultiplier(self.unit[future_price[k].commodity])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
                        if self.tcost:
                            tr_r.setTcost(**self.tcost_list[k])
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)


                    elif abs(holdings[i]) < abs(holdings[i-1]) and holdings[i] * holdings[i-1] >= 0:

                        # 减仓或平仓
                        needed_covered = abs(holdings[i - 1]) - abs(holdings[i])  # 需要减仓的数量
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
                                            self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
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
                                        tr_r.setCommodity(future_price[k].commodity)
                                        tr_r.setVolume(uncovered_vol)
                                        tr_r.setMultiplier(self.unit[future_price[k].commodity])
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
                                            self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
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
                                            self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
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

                    elif holdings[i] * holdings[i-1] < 0:

                        # 先平仓后开仓
                        needed_covered = abs(holdings[i - 1])  # 需要减仓的数量
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
                                            self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
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
                                            self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
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
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[future_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenDT(self.dt[i])
                        tr_r.setCommodity(future_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]))
                        tr_r.setMultiplier(self.unit[future_price[k].commodity])
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
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

    def displayResult(self, holdingsObj, saveLocal=True):
        # saveLocal是逻辑变量，是否将结果存在本地


        pnl, margin_occ, value, turnover_rate = self.getPnlDaily(holdingsObj)
        # print 'nv'
        # df_pnl = pd.DataFrame(np.cumsum(pnl), index=self.dt)
        # df_pnl.to_clipboard()
        nv = 1. + np.cumsum(pnl) / self.capital  # 转换成初始净值为1
        margin_occ_ratio = margin_occ / (self.capital + np.cumsum(pnl))
        # leverage = value / (self.capital + np.cumsum(pnl))
        leverage = value / self.capital
        trade_record = self.statTrade(holdingsObj)
        print('==============================回测结果================================')
        self.calcIndicatorByYear(nv, turnover_rate)

        trade_pnl = []
        for tr in trade_record:
            trade_pnl.extend([t.pnl for t in trade_record[tr]])

        if saveLocal:
            current_file = sys.argv[0]
            save_path = os.path.splitext(current_file)[0]
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            # 保存持仓为holdings.csv
            holdings_df = holdingsObj.to_frame()
            holdings_df.to_csv(os.path.join(save_path, 'holdings.csv'))

            # 保存总的回测结果为result.csv
            total_df = pd.DataFrame({u'每日PnL': pnl, u'净值': nv, u'资金占用比例': margin_occ_ratio,
                                     u'杠杆倍数': leverage, '换手率': turnover_rate}, index=self.dt)
            total_df.to_csv(os.path.join(save_path, 'result.csv'), encoding='utf-8')

            # 保存交易记录为trade_detail.csv
            detail_df = pd.DataFrame()
            for k in trade_record:
                detail_df = pd.concat([detail_df, pd.DataFrame.from_records([tr.__dict__ for tr in trade_record[k]])],
                                      ignore_index=True)
            detail_df.to_csv(os.path.join(save_path, 'details.csv'))

            # 保存最新的持仓
            new_holdings = pd.DataFrame.from_dict(holdingsObj.get_newest_holdings(), orient='index', columns=['HOLDINGS'])
            new_holdings.sort_values(by='HOLDINGS', ascending=False, inplace=True)
            new_holdings.to_csv(os.path.join(save_path, 'new_holdings_%s.csv' % datetime.now().strftime('%y%m%d')))

        trade_pnl = np.array(trade_pnl)

        register_matplotlib_converters()
        plt.subplot(511)
        plt.plot_date(self.dt, nv, fmt='-r', label='PnL')
        plt.grid()
        plt.legend()

        plt.subplot(512)
        plt.hist(trade_pnl[~np.isnan(trade_pnl)], bins=50, label='DistOfPnL', color='r')
        plt.legend()
        plt.grid()

        plt.subplot(513)
        plt.plot_date(self.dt, margin_occ_ratio, fmt='-r', label='margin_occupation_ratio')
        plt.grid()
        plt.legend()


        plt.subplot(514)
        plt.plot_date(self.dt, leverage, fmt='-r', label='leverage')
        plt.grid()
        plt.legend()

        plt.subplot(515)
        plt.plot_date(self.dt, turnover_rate, fmt='-r', label='turnover_rate')
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

    def calcIndicatorByYear(self, net_value, turnover_rate):

        # 分年度进行统计，这里需要注意，标准差的计算是使用的无偏

        nv = pd.DataFrame(net_value, index=self.dt, columns=['NV'])

        turnover_df = pd.DataFrame(turnover_rate, index=self.dt, columns=['Turnover'])

        # ############ 总的统计结果 ################

        dd = nv - nv.expanding().max()
        mdd = dd.min().values[0]
        mdd_end = dd.idxmin().values[0]
        mdd_start = nv.loc[:mdd_end].idxmax().values[0]
        # rtn_daily = nv.pct_change()  # 这个是复利日收益
        rtn_daily = nv.diff()  # 这个是单利日收益

        total_df = pd.DataFrame({'AnnualRtn': rtn_daily.mean().values[0] * 250.,
                                 'AnnualVol': rtn_daily.std().values[0] * np.sqrt(250.),
                                 'Sharpe': rtn_daily.mean().values[0] * np.sqrt(250.) / rtn_daily.std().values[0],
                                 'MaxDrawdown': mdd,
                                 'MaxDDStart': mdd_start,
                                 'MaxDDEnd': mdd_end,
                                 'Days': nv.count().values[0],
                                 'NetValueInit': nv['NV'].iloc[0],
                                 'NetValueFinal': nv['NV'].iloc[-1],
                                 'Turnover': turnover_df.mean().values[0]}, index=['total'])

        # ################# 按年度统计得到的结果 #####################

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

        turnover_df['year'] = [i.year for i in turnover_df.index]
        turnover_grouped = turnover_df.groupby(by='year')
        turnover_mean = turnover_grouped.mean()

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
        res_df = res_df.join(turnover_mean, how='outer')
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

    test1 = DataClass(nm='TA.CZC', dt=np.arange(10))
    a = np.random.randn(10)
    test1.add_ts_data(obj_name='CLOSE', obj_value=a)
    test1.rearrange_ts_data(np.arange(20))
    print(getattr(test1, 'CLOSE'))
    # test1.check_len()

    # ttest1 = TradeRecordByTimes()
    # ttest1.setDT('20181203')
    # ttest1.setContract('TA1901.CZC')
    # ttest1.setPrice(6000)
    # ttest1.setVolume(10)
    # ttest1.setDirection(1)
    # ttest1.setType(1)
    # ttest1.setMultiplier(5)
    # ttest1.setMarginRatio(0.1)
    # ttest1.calMarginOccupation()
    #
    # # tdtest = TradeRecordByDay(ttest1)
    #
    # print(ttest1)
    #
    # a1 = {'TA1901.CZC': {}}
    # a2 = dict()
    # a = TradeRecordByDay(dt='20181203', holdPosDict=a2, MkData=a2, newTrade=[])
    # a.addNewPositon()
    # print(a.getFinalMK())
    # print(a.getHoldPosition())
    # # a = BacktestSys()
    # # print a.net_value
    # # print a.calcSharpe()
    # # print a.calcMaxDrawdown()
    # # a.generateNV()
    # # print a.calcBTResult()
    # # plt.plot(a.net_value)
    # # plt.show()
    # # a.start_date = '20180101'
    # # a.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC')
