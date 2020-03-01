
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
from .dObject import DataClass
from .hObject import HoldingClass
from .tObject import TradeRecordByTimes, TradeRecordByTrade, TradeRecordByDay

class BacktestSys(object):

    def __init__(self):
        self.current_file = sys.argv[0]
        self.prepare()

    def prepare(self):

        pd.set_option('expand_frame_repr', True)
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', 200)

        # 从配置文件里提取一些公用的信息
        rel_path = os.path.split(os.path.relpath(__file__))[0]
        config_path = os.path.join(rel_path, '..', 'config')

        # 从multiplier.yaml中提取合约乘数信息
        multiplier_yaml = open(os.path.join(config_path, 'multiplier.yaml'))
        multiplier = yaml.load(multiplier_yaml, Loader=yaml.FullLoader)

        # 从marginratio.yaml中提取保证金比例信息
        marginratio_yaml = open(os.path.join(config_path, 'marginratio.yaml'))
        margin_ratio = yaml.load(marginratio_yaml, Loader=yaml.FullLoader)

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

        # 判断是否需要mongo的连接信息，如果yaml里没有mongo_usage，则默认是需要的
        mongo_usage = self.conf.setdefault('mongo_usage', 1)

        # 配置Mongo信息
        if mongo_usage:
            mongo_yaml = open(os.path.join(config_path, 'mongoconfig.yaml'))
            mongo_config = yaml.load(mongo_yaml, Loader=yaml.FullLoader)
            host = mongo_config['host']
            port = mongo_config['port']
            usr = mongo_config['user']
            pwd = mongo_config['pwd']
            db_nm = mongo_config['db_name']
            conn = pymongo.MongoClient(host=host, port=port)
            self.db = conn[db_nm]
            self.db.authenticate(name=usr, password=pwd)

        # 判断是否需要tcost的信息，如果yaml里没有tcost，则默认是不需要的
        tcost = self.conf.setdefault('tcost', 0)
        if tcost:
            tcost_yaml = open(os.path.join(config_path, 'tcost.yaml'))
            tcost_config = yaml.load(tcost_yaml, Loader=yaml.FullLoader)


        self.turnover = self.conf['turnover']

        self.data = {}

        # 将需要的数据信息存到字典self.data中，key是yaml中配置的数据分类，比如bt_price，value仍然是个字典，{品种:数据实例}
        # e.g. self.data = {'future_price': {'TA.CZC': DataClass的实例}}

        raw_data = self.conf['data']

        self.exchange_func = {}
        exchange_list = []

        for sub_class, sub_data in raw_data.items():
            self.data[sub_class] = {}
            for d in sub_data:

                # 根据数据需要的属性进行默认设置
                d.setdefault('frequency', 'daily')
                d.setdefault('unit_change', 'unchange')

                exchange_list.append(d['unit_change'])

                # 如果该数据是在bt_price中，则是用于计算pnl
                if sub_class == 'bt_price':

                    # 增加了该合约的合约乘数，如果没有，则为1
                    if 'commodity' in d and d['commodity'] in multiplier:
                        d.setdefault('multiplier', multiplier[d['commodity']])
                    else:
                        d.setdefault('multiplier', 1)

                    # 如果在yaml文件中没有提供保证金比例，则增加该合约的保证金比例，如果没有，则为1
                    if 'commodity' in d and d['commodity'] in margin_ratio:
                        d.setdefault('margin_ratio', margin_ratio[d['commodity']])
                    else:
                        d.setdefault('margin_ratio', 1)


                    # 增加该合约的交易成本，如果没有，则为0
                    if tcost:
                        if 'commodity' in d and d['commodity'] in tcost_config:
                            d.setdefault('cost', tcost_config[d['commodity']])
                        else:
                            d.setdefault('cost', 0)

                    d.setdefault('switch', 0)  # 是否有合约切换的情况，默认不做
                    d.setdefault('bt_mode', 'OPEN')  # 回测的方式，默认为OPEN回测

                    # d.setdefault('tcost', 0)
                    # d.setdefault('margin_ratio', 1)
                    # d.setdefault('trade_unit', 1)
                    # if 'tcost' in d and d['tcost'] == 1 and 'cost_mode' not in d:
                    #     d['tcost'] = 0

                self.data[sub_class][d['name']] = DataClass(nm=d['name'], freq=d['frequency'])
                for k, v in d.items():
                    print(k, v)
                    self.data[sub_class][d['name']].add_data(k, v)

                # 读取数据
                # 如果是从CSV中提取，暂时不能将切换合约的问题解决掉，CSV主要是用于分钟数据的分析
                if d['source'] == 'CSV':
                    data_df = pd.read_csv(d['csv_path'], index_col=0, parse_dates=True)
                    data_df = data_df[d['fields']]
                    data_df = data_df.loc[(self.start_dt <= data_df.index) & (data_df.index <= self.end_dt)]
                    if len(data_df.columns) != len(d['fields']):
                        raise Exception('%s数据的fields字段有错误，请检查' % d['name'])
                    self.data[sub_class][d['name']].add_ts(data_df.index.to_pydatetime())
                    for f in d['fields']:
                        self.data[sub_class][d['name']].add_ts_data(f, data_df[f].to_numpy())

                elif d['source'] == 'MONGO':
                    table = self.db[d['collection']] if 'collection' in d else self.db['FuturesMD']
                    query_arg = d['db_query']
                    query_arg.update({'date': {'$gte': self.start_dt, '$lte': self.end_dt}})
                    projection_fields = ['date'] + d['fields']

                    if 'switch' in d and d['switch'] == 1:
                        projection_fields = projection_fields + ['switch_contract', 'specific_contract']

                    res = table.find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)

                    df_res = pd.DataFrame.from_records(res)
                    df_res.drop(columns='_id', inplace=True)

                    dict_res = df_res.to_dict(orient='list')
                    self.data[sub_class][d['name']].add_ts(dict_res['date'])

                    for k, v in dict_res.items():
                        if k != 'date' and k != 'specific_contract' and k != 'date_time':
                            self.data[sub_class][d['name']].add_ts_data(k, v)
                        elif k == 'specific_contract':
                            self.data[sub_class][d['name']].add_ts_string(k, v)

        # 需要导入美元兑人民币数据，但是只有日度数据
        if 'dollar' in exchange_list:
            self.exchange_func['dollar'] = 'dollar2rmb'
            query_arg = {'wind_code': 'M0067855', 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date', 'CLOSE']
            res = self.db['EDB'].find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            dict_res = df_res.to_dict(orient='list')
            self.dollar2rmb = DataClass(nm=self.exchange_func['dollar'])
            self.dollar2rmb.add_ts(dict_res['date'])
            for k, v in dict_res.items():
                if k != 'date':
                    self.dollar2rmb.add_ts_data(k, v)

        # 将提取的数据按照交易时间的并集重新生成日期
        # 同时对于不是日度数据的数据进行处理
        date_set = set()
        for sub_class, sub_data in self.data.items():
            for d in sub_data:
                # 分钟数据的处理
                self.data[sub_class][d].min_2_dt()
                # 频率大于日度的数据的处理
                self.data[sub_class][d].long_2_dt()
                date_set = date_set.union(sub_data[d].dt)
                print(self.data[sub_class][d].__dict__)
        self.dt = np.array(list(date_set))
        self.dt.sort()

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
                # 对于分钟数据不需要进行这样的处理，通常是量价数据一定是在交易时段，如果处理的话比较耗时间
                if self.data[sub_class][d].frequency != 'minutes':
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
            self.unchange.add_ts(self.dt)
            self.unchange.add_ts_data('CLOSE', np.ones(len(self.dt)))

    def strategy(self):
        raise NotImplementedError

    # 检查持仓的品种是否有行情数据
    def holdingsCheck(self, holdingsObj):
        return (np.in1d(holdingsObj.asset, list(self.data['bt_price'].keys()))).all()

    def holdingsProcess(self, holdingsObj):
        """根据配置文件对持仓进行最后的处理"""
        if not self.holdingsCheck(holdingsObj):
            raise Exception('持仓品种没有行情数据，请检查字段是否正确')
        # 对生成的持仓进行处理，需要注意的是这个函数要最后再用
        # 如果在配置文件中的持仓周期大于1，需要对持仓进行调整。通常该参数是针对alpha策略。
        if self.turnover > 1:
            print('根据turnover对持仓进行调整，请检查是否为alpha策略')
            holdingsObj.adjust_holdings_turnover(self.turnover)

        for h in holdingsObj.asset:
            if self.data['bt_price'][h].bt_mode in ['OPEN', 'open']:
                # 如果是开盘价进行交易，则将初始持仓向后平移一位
                holdingsObj.shift_holdings(mode='single', label=h)


        # 如果当天合约没有交易量，那么需要对权重进行调整
        for h in holdingsObj.asset:
            h_holdings = getattr(holdingsObj, h)
            if hasattr(self.data['bt_price'][h], 'CLOSE'):
                h_cls = self.data['bt_price'][h].CLOSE
            else:
                h_cls = getattr(self.data['bt_price'][h], self.data['bt_price'][h].bt_mode)

            if hasattr(self.data['bt_price'][h], 'OPEN'):
                h_opn = self.data['bt_price'][h].OPEN

            for i in range(1, len(self.dt)):
                # 如果之前合约没有上市，将之前的持仓全部赋为0
                if np.isnan(h_cls[:i+1]).all():
                    h_holdings[i] = 0
                    continue
                if (np.isnan(h_cls[i]) or (hasattr(self.data['bt_price'][h], 'OPEN') and np.isnan(h_opn[i]))) and \
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
        mode=4: 不加杠杆。多空的资金相同，各个方向里的持仓品种的合约价值相同。
        """
        if not self.holdingsCheck(holdingsObj):
            raise Exception('持仓品种没有行情数据，请检查字段是否正确')

        # 合约持仓的dataframe
        holdings_df = holdingsObj.to_frame()

        # 计算出各合约做1手的合约价值
        cls_df = pd.DataFrame()
        for k, v in self.data['bt_price'].items():
            if hasattr(v, 'CLOSE') and (v.bt_mode == 'OPEN' or v.bt_mode == 'CLOSE'):
                price = v.CLOSE
            else:
                price = getattr(v, v.bt_mode)
            cls_temp = price * v.trade_unit
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
            for k, v in self.data['bt_price'].items():
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

        elif mode == 4:
            # 根据是否多空，分配资金。如果有多空，则各一半，否则为全部
            capital_buy_sell = holdings_df.apply(
                func=lambda x: self.capital / 2. if (x > 0).any() * (x < 0).any() else self.capital,
                axis=1, raw=True)
            # 做多的个数和做空的个数
            num_buy = holdings_df.apply(func=lambda x: len(x[x > 0]), axis=1, raw=True)
            num_sell = holdings_df.apply(func=lambda x: len(x[x < 0]), axis=1, raw=True)
            capital_buy = pd.DataFrame()
            capital_sell = pd.DataFrame()
            for c in holdings_df:
                capital_buy[c] = capital_buy_sell / num_buy
                capital_sell[c] = capital_buy_sell / num_sell

            capital = pd.DataFrame(0, index=holdings_df.index, columns=holdings_df.columns)
            capital[holdings_df > 0] = capital_buy[holdings_df > 0]
            capital[holdings_df < 0] = capital_sell[holdings_df < 0]

            cls_df = cls_df * np.sign(holdings_df)
            cls_df[cls_df == 0] = np.nan

            holdings_new = capital / cls_df
            holdings_new.fillna(0, inplace=True)
            holdings_new = holdings_new.round(decimals=0)
            for h in holdingsObj.asset:
                holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
            return holdingsObj

        elif mode == 5:
            # 根据是否多空，分配资金。如果有多空，则各一半，否则为全部
            capital_buy_sell = holdings_df.apply(
                func=lambda x: self.capital / 2. if (x > 0).any() * (x < 0).any() else self.capital,
                axis=1, raw=True)
            # 做多的个数和做空的个数
            num_buy = holdings_df.apply(func=lambda x: len(x[x > 0]), axis=1, raw=True)
            num_sell = holdings_df.apply(func=lambda x: len(x[x < 0]), axis=1, raw=True)
            capital_buy = pd.DataFrame()
            capital_sell = pd.DataFrame()
            for c in holdings_df:
                capital_buy[c] = capital_buy_sell / num_buy
                capital_sell[c] = capital_buy_sell / num_sell

            capital = pd.DataFrame(0, index=holdings_df.index, columns=holdings_df.columns)
            capital[holdings_df > 0] = capital_buy[holdings_df > 0]
            capital[holdings_df < 0] = capital_sell[holdings_df < 0]

            cls_df = cls_df * np.sign(holdings_df)
            cls_df[cls_df == 0] = np.nan

            holdings_new = capital / cls_df
            holdings_new.fillna(0, inplace=True)

            # 判断初始持仓是否与前一天的初始持仓相同
            holdings_yestd = holdings_df.shift(periods=1)
            holdings_equal = np.sign(holdings_df) == np.sign(holdings_yestd)

            # 分别统计多仓和空仓的当天与前一天的持仓个数是否相同
            num_buy_yestd = num_buy.shift(periods=1)
            num_sell_yestd = num_sell.shift(periods=1)

            num_buy_equal = num_buy == num_buy_yestd
            num_sell_equal = num_sell == num_sell_yestd

            num_buy_equal_df = pd.DataFrame()
            num_sell_equal_df = pd.DataFrame()
            for c in holdings_df:
                num_buy_equal_df[c] = num_buy_equal
                num_sell_equal_df[c] = num_sell_equal

            holdings_equal[(holdings_df > 0) & ~num_buy_equal_df] = False
            holdings_equal[(holdings_df < 0) & ~num_sell_equal_df] = False

            holdings_new[holdings_equal] = np.nan
            holdings_new.fillna(method='ffill', inplace=True)
            holdings_new = holdings_new.round(decimals=0)
            for h in holdingsObj.asset:
                holdingsObj.update_holdings(h, holdings_new[h].values.flatten())
            return holdingsObj

        elif mode == 6:

            # 根据持仓得到每日的持有几个合约，针对每个合约平均分配资金
            # 这里对单个合约的持仓有限制，不能超过总的1/6
            holdings_num = np.abs(np.sign(holdings_df))
            holdings_num = holdings_num.sum(axis=1)
            holdings_num[holdings_num < 6] = 6
            holdings_num[holdings_num == 0] = np.nan

            sub_capital = self.capital / holdings_num

            cls_df = cls_df * np.sign(holdings_df)
            cls_df[cls_df == 0] = np.nan

            holdings_new = pd.DataFrame()
            for c in cls_df:
                holdings_new[c] = sub_capital / cls_df[c]

            holdings_new.fillna(0, inplace=True)

            # 判断初始持仓是否与前一天的初始持仓相同
            holdings_dir = np.sign(holdings_df)
            holdings_yestd = holdings_dir.shift(periods=1)
            holdings_equal = holdings_dir == holdings_yestd

            # 统计当天持仓的合约个数
            # holdings_temp = holdings_df.copy(deep=True)
            # holdings_temp[holdings_temp == 0] = np.nan
            # holdings_num = holdings_temp.count(axis=1)
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
        根据持仓情况统计每日换手率，即成交的合约价值/总资金，如果不加杠杆应该是在0-2之间
        需要注意的一点是，如果换合约的时候，没有成交量，使用的是之前的价格进行的平仓，导致的回测的pnl和换手不准确，暂时无法解决
        '''

        pnl_daily = np.zeros_like(self.dt).astype('float')
        margin_occ_daily = np.zeros_like(self.dt).astype('float')
        value_daily = np.zeros_like(self.dt).astype('float')
        turnover_daily = np.zeros_like(self.dt).astype('float')

        holdpos = {}

        bt_price = self.data['bt_price']

        for i, v in enumerate(self.dt):
            newtradedaily = []
            mkdata = {}
            for h in holdingsObj.asset:
                if hasattr(bt_price[h], 'CLOSE'):
                    price = getattr(bt_price[h], 'CLOSE')
                else:
                    price = getattr(bt_price[h], bt_price[h].bt_mode)
                cls_td = price[i]
                exrate_td = getattr(self, self.exchange_func[bt_price[h].unit_change]).CLOSE[i]
                holdings_td = getattr(holdingsObj, h)[i]

                # 如果在当前日期该合约没有开始交易，则直接跳出当前循环，进入下一个合约
                if np.isnan(price[:i+1]).all():
                    continue
                if np.isnan(cls_td):
                    print('%s合约在%s这一天没有收盘数据' % (h, v.strftime('%Y%m%d')))
                    continue
                # 需要传入的市场数据
                mkdata[h] = {'CLOSE': cls_td,
                             'ExRate': exrate_td,
                             'multiplier': bt_price[h].trade_unit,
                             'margin_ratio': bt_price[h].margin_ratio}

                # 合约首日交易便有持仓时
                if i == 0 or np.isnan(price[:i]).all():
                    if holdings_td != 0:
                        newtrade = TradeRecordByTimes()
                        newtrade.setTT(v)
                        newtrade.setContract(h)
                        newtrade.setCommodity(bt_price[h].commodity)
                        newtrade.setPrice(getattr(bt_price[h], bt_price[h].bt_mode)[i])
                        newtrade.setExchangRate(exrate_td)
                        newtrade.setType(1)
                        newtrade.setVolume(abs(holdings_td))
                        newtrade.setMarginRatio(bt_price[h].margin_ratio)
                        newtrade.setMultiplier(bt_price[h].trade_unit)
                        newtrade.setDirection(np.sign(holdings_td))
                        if bt_price[h].tcost:
                            newtrade.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                        newtrade.calCost()
                        turnover_daily[i] += newtrade.calValue()
                        newtradedaily.append(newtrade)

                # 如果不是第一天交易的话，需要前一天的收盘价
                elif i != 0:
                    holdings_ystd = getattr(holdingsObj, h)[i - 1]
                    mkdata[h]['PRECLOSE'] = price[i - 1]
                    mkdata[h]['PRECLOSE_ExRate'] = getattr(self, self.exchange_func[bt_price[h].unit_change]).CLOSE[i - 1]
                    if np.isnan(mkdata[h]['PRECLOSE']):
                        for pre_counter in np.arange(2, i + 1):
                            if ~np.isnan(price[i - pre_counter]):
                                mkdata[h]['PRECLOSE'] = price[i - pre_counter]
                                mkdata[h]['PRECLOSE_ExRate'] = getattr(
                                    self, self.exchange_func[bt_price[h].unit_change]).CLOSE[i - pre_counter]
                                print('%s合约在%s使用的PRECLOSE是%d天前的收盘价' % (h, self.dt[i].strftime('%Y%m%d'), pre_counter))
                                break

                    # 如果切换主力合约
                    if bt_price[h].switch:
                        mkdata[h].update({'switch_contract': bt_price[h].switch_contract[i],
                                          'specific_contract': bt_price[h].specific_contract[i - 1]})
                        # 如果switch_contract为True，需要前主力合约的OPEN
                        if mkdata[h]['switch_contract'] and ~np.isnan(mkdata[h]['switch_contract']):
                            # 这里需要注意的是np.nan也会判断为true，所以需要去掉
                            # 比如MA.CZC在刚开始的时候是没有specific_contract
                            # 这里没有针对现货价格进行提取，因为涉及到切换合约只能是期货价格
                            # if np.isnan(mkdata[h]['switch_contract']):
                            #     raise Exception('adsfasdfasdfsafsa')
                            queryArgs = {'wind_code': mkdata[h]['specific_contract'], 'date': self.dt[i]}
                            projectionField = ['OPEN']
                            table = self.db['FuturesMD']
                            if mkdata[h]['specific_contract'] == 'nan':
                                # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                                print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                                      (h, self.dt[i].strftime('%Y%m%d')))
                                old_open = price[i - 1]
                                old_open_exrate = getattr(self, self.exchange_func[bt_price[h].unit_change]).CLOSE[i - 1]
                            elif table.find_one(queryArgs, projectionField):
                                old_open = table.find_one(queryArgs, projectionField)['OPEN']
                                old_open_exrate = getattr(self, self.exchange_func[bt_price[h].unit_change]).CLOSE[i]
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

                    if bt_price[h].switch and mkdata[h]['switch_contract'] and ~np.isnan(mkdata[h]['switch_contract'])\
                            and holdings_ystd != 0:
                        newtrade1 = TradeRecordByTimes()
                        newtrade1.setTT(v)
                        newtrade1.setContract(h)
                        newtrade1.setCommodity(bt_price[h].commodity)
                        newtrade1.setPrice(old_open)
                        newtrade1.setExchangRate(old_open_exrate)
                        newtrade1.setVolume(abs(holdings_ystd))
                        newtrade1.setMarginRatio(bt_price[h].margin_ratio)
                        newtrade1.setMultiplier(bt_price[h].trade_unit)
                        newtrade1.setDirection(-np.sign(holdings_ystd))
                        if bt_price[h].tcost:
                            newtrade1.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                        newtrade1.calCost()
                        turnover_daily[i] += newtrade1.calValue()
                        newtradedaily.append(newtrade1)

                        if holdings_td != 0:
                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setTT(v)
                            newtrade2.setContract(h)
                            newtrade2.setCommodity(bt_price[h].commodity)
                            newtrade2.setPrice(getattr(bt_price[h], bt_price[h].bt_mode)[i])
                            newtrade2.setExchangRate(exrate_td)
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(holdings_td))
                            newtrade2.setMarginRatio(bt_price[h].margin_ratio)
                            newtrade2.setMultiplier(bt_price[h].trade_unit)
                            newtrade2.setDirection(np.sign(holdings_td))
                            if bt_price[h].tcost:
                                newtrade2.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                            newtrade2.calCost()
                            turnover_daily[i] += newtrade2.calValue()
                            newtradedaily.append(newtrade2)

                    else:
                        if holdings_td * holdings_ystd < 0:
                            newtrade1 = TradeRecordByTimes()
                            newtrade1.setTT(v)
                            newtrade1.setContract(h)
                            newtrade1.setCommodity(bt_price[h].commodity)
                            newtrade1.setPrice(getattr(bt_price[h], bt_price[h].bt_mode)[i])
                            newtrade1.setExchangRate(exrate_td)
                            newtrade1.setType(-1)
                            newtrade1.setVolume(abs(holdings_ystd))
                            newtrade1.setMarginRatio(bt_price[h].margin_ratio)
                            newtrade1.setMultiplier(bt_price[h].trade_unit)
                            newtrade1.setDirection(np.sign(holdings_td))
                            if bt_price[h].tcost:
                                newtrade1.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                            newtrade1.calCost()
                            turnover_daily[i] += newtrade1.calValue()
                            newtradedaily.append(newtrade1)

                            newtrade2 = TradeRecordByTimes()
                            newtrade2.setTT(v)
                            newtrade2.setContract(h)
                            newtrade2.setCommodity(bt_price[h].commodity)
                            newtrade2.setPrice(getattr(bt_price[h], bt_price[h].bt_mode)[i])
                            newtrade2.setExchangRate(exrate_td)
                            newtrade2.setType(1)
                            newtrade2.setVolume(abs(holdings_td))
                            newtrade2.setMarginRatio(bt_price[h].margin_ratio)
                            newtrade2.setMultiplier(bt_price[h].trade_unit)
                            newtrade2.setDirection(np.sign(holdings_td))
                            if bt_price[h].tcost:
                                newtrade2.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                            newtrade2.calCost()
                            turnover_daily[i] += newtrade2.calValue()
                            newtradedaily.append(newtrade2)

                        elif holdings_td == holdings_ystd:  # 没有交易
                            continue

                        else:
                            newtrade = TradeRecordByTimes()
                            newtrade.setTT(v)
                            newtrade.setContract(h)
                            newtrade.setCommodity(bt_price[h].commodity)
                            newtrade.setPrice(getattr(bt_price[h], bt_price[h].bt_mode)[i])
                            newtrade.setExchangRate(exrate_td)
                            newtrade.setType(np.sign(abs(holdings_td) - abs(holdings_ystd)))
                            newtrade.setVolume(abs(holdings_td - holdings_ystd))
                            newtrade.setMarginRatio(bt_price[h].margin_ratio)
                            newtrade.setMultiplier(bt_price[h].trade_unit)
                            newtrade.setDirection(np.sign(holdings_td - holdings_ystd))
                            if bt_price[h].tcost:

                                newtrade.setCost(bt_price[h].cost_mode, bt_price[h].cost_value)
                            newtrade.calCost()
                            turnover_daily[i] += newtrade.calValue()
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

        bt_price = self.data['bt_price']
        for k in holdingsObj.asset:
            trade_record[k] = []
            uncovered_record[k] = []
            count = 0
            trade_price = getattr(bt_price[k], bt_price[k].bt_mode)
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
                if bt_price[k].switch and bt_price[k].switch_contract[i] \
                    and ~np.isnan(bt_price[k].switch_contract[i]) and holdings[i - 1] != 0:

                    # 条件中需要加上判断合约是否为nan，否则也会进入到该条件中

                    table = self.db['FuturesMD']
                    res = bt_price[k].specific_contract[i-1]
                    # 对于换合约需要平仓的合约均使用开盘价进行平仓
                    queryArgs = {'wind_code': res, 'date': self.dt[i]}
                    projectionField = ['OPEN']

                    if res == 'nan':
                        # 对于MA.CZC, ZC.CZC的品种，之前没有specific_contract字段，使用前一交易日的收盘价
                        print('%s在%s的前一交易日没有specific_contract字段，使用前一交易日的收盘价换约平仓' % \
                              (k, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = bt_price[k].CLOSE[i-1]
                        trade_exrate_switch = getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i-1]
                    elif table.find_one(queryArgs, projectionField):
                        trade_price_switch = table.find_one(queryArgs, projectionField)['OPEN']
                        trade_exrate_switch = getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i]
                        if np.isnan(trade_price_switch):
                            print('%s因为该合约当天没有交易，在%s使用前一天的收盘价作为换约平仓的价格' % \
                                  (res, self.dt[i].strftime('%Y%m%d')))
                            trade_price_switch = bt_price[k].CLOSE[i-1]
                            trade_exrate_switch = getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i - 1]
                    else:
                        print('%s因为已经到期，在%s使用的是前一天的收盘价作为换约平仓的价格' % \
                              (res, self.dt[i].strftime('%Y%m%d')))
                        trade_price_switch = bt_price[k].CLOSE[i-1]
                        trade_exrate_switch = getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i - 1]

                    if uncovered_record[k]:
                        needed_covered = abs(holdings[i - 1])
                        uncovered_record_sub = []
                        for j in np.arange(1, len(uncovered_record[k]) + 1):
                            for m in np.arange(1, len(trade_record[k]) + 1):
                                if uncovered_record[k][-j] == trade_record[k][-m].count:
                                    trade_record[k][-m].setClose(trade_price_switch)
                                    trade_record[k][-m].setCloseExchangeRate(trade_exrate_switch)
                                    trade_record[k][-m].setCloseTT(self.dt[i])
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
                            tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                            tr_r.setOpenTT(self.dt[i])
                            tr_r.setCommodity(bt_price[k].commodity)
                            tr_r.setVolume(abs(holdings[i]))
                            tr_r.setMultiplier(bt_price[k].trade_unit)
                            tr_r.setContract(k)
                            tr_r.setDirection(np.sign(holdings[i]))
                            if bt_price[k].tcost:
                                tr_r.setTcost(bt_price[k].cost_mode, bt_price[k].cost_value)
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
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenTT(self.dt[i])
                        tr_r.setCommodity(bt_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]))
                        tr_r.setMultiplier(bt_price[k].trade_unit)
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
                        if bt_price[k].tcost:
                            tr_r.setTcost(bt_price[k].cost_mode, bt_price[k].cost_value)
                        trade_record[k].append(tr_r)
                        uncovered_record[k].append(tr_r.count)

                    elif abs(holdings[i]) > abs(holdings[i-1]) and holdings[i] * holdings[i-1] >= 0:
                        # 新开仓或加仓

                        count += 1
                        tr_r = TradeRecordByTrade()
                        tr_r.setCounter(count)
                        tr_r.setOpen(trade_price[i])
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenTT(self.dt[i])
                        tr_r.setCommodity(bt_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]) - abs(holdings[i-1]))
                        tr_r.setMultiplier(bt_price[k].trade_unit)
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
                        if bt_price[k].tcost:
                            tr_r.setTcost(bt_price[k].cost_mode, bt_price[k].cost_value)
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
                                            self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                                        trade_record[k][-m].setCloseTT(self.dt[i])
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
                                        tr_r.setOpenTT(trade_record[k][-m].open_dt)
                                        tr_r.setCommodity(bt_price[k].commodity)
                                        tr_r.setVolume(uncovered_vol)
                                        tr_r.setMultiplier(bt_price[k].trade_unit)
                                        tr_r.setContract(k)
                                        tr_r.setDirection(trade_record[k][-m].direction)
                                        if bt_price[k].tcost:
                                            tr_r.setTcost(bt_price[k].cost_mode, bt_price[k].cost_value)
                                        trade_record[k].append(tr_r)
                                        uncovered_record_add.append(tr_r.count)

                                        break

                                    # 如果需要减仓的数量等于最近的开仓数量
                                    elif needed_covered == trade_record[k][-m].volume:
                                        trade_record[k][-m].setClose(trade_price[i])
                                        trade_record[k][-m].setCloseExchangeRate(getattr(
                                            self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                                        trade_record[k][-m].setCloseTT(self.dt[i])
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
                                            self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                                        trade_record[k][-m].setCloseTT(self.dt[i])
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
                                            self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                                        trade_record[k][-m].setCloseTT(self.dt[i])
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
                                            self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                                        trade_record[k][-m].setCloseTT(self.dt[i])
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
                        tr_r.setOpenExchangeRate(getattr(self, self.exchange_func[bt_price[k].unit_change]).CLOSE[i])
                        tr_r.setOpenTT(self.dt[i])
                        tr_r.setCommodity(bt_price[k].commodity)
                        tr_r.setVolume(abs(holdings[i]))
                        tr_r.setMultiplier(bt_price[k].trade_unit)
                        tr_r.setContract(k)
                        tr_r.setDirection(np.sign(holdings[i]))
                        if bt_price[k].tcost:
                            tr_r.setTcost(bt_price[k].cost_mode, bt_price[k].cost_value)
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

    def calcIndicatorByYear(self, net_value, turnover_rate, show=True):

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

        if show:
            print(res_df)

        return res_df

    def getTotalResult(self, holdingsObj, show=True):

        pnl, margin_occ, value, turnover_rate = self.getPnlDaily(holdingsObj)
        nv = 1. + np.cumsum(pnl) / self.capital  # 转换成初始净值为1
        res = self.calcIndicatorByYear(nv, turnover_rate, show=show)
        return res


if __name__ == '__main__':



    t1 = DataClass(nm='TA.CZC', dt=np.arange(10))
    a = np.random.randn(10)
    t1.add_ts_data(obj_name='CLOSE', obj_value=a)
    t1.rearrange_ts_data(np.arange(20))
    print(getattr(t1, 'CLOSE'))
    # test1.check_len()

    # ttest1 = TradeRecordByTimes()
    # ttest1.setTT('20181203')
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
