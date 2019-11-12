"""
根据基本面的研究生成各品种的利润率数据，并入库
这里需要对数据的时间进行平移
如果都是期货的数据不需要平移
如果都是现货的数据不需要平移
如果期货和现货或者路透的外盘数据，则需要将现货和路透的数据向后平移一天
"""

import pymongo
import pandas as pd
from datetime import datetime
import sys
import logging


class ProfitRate(object):

    def __init__(self):

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(filename)s %(funcName)s %(levelname)s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S %a')

        fh = logging.FileHandler('E:\\CBNB\\BackTestSystem3\\data_saving.log')
        ch = logging.StreamHandler()
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


        self.conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
        self.db = self.conn['CBNB']
        self.db.authenticate(name='yuanjie', password='yuanjie')
        self.target_coll = self.db['ProfitRate']
        self.futures_coll = self.db['FuturesMD']
        self.spot_coll = self.db['SpotMD']
        self.edb_coll = self.db['EDB']

    def calc_ll_profit_rate(self, method='future'):
        """LL的利润公式：LL - (MOPJ + 380) * 1.13 * 1.065 * 美元兑人民币 - 150"""
        if method == 'future':
            queryArgs = {'tr_code': 'NACFRJPSWMc1'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            mopj = pd.DataFrame.from_records(records, index='date')
            mopj.drop(columns=['_id'], inplace=True)
            mopj.rename(columns={'CLOSE': 'MOPJ'}, inplace=True)

            queryArgs = {'wind_code': 'L.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'CLOSE': 'L.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'tr_code': 'NAF-SIN'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            mopj = pd.DataFrame.from_records(records, index='date')
            mopj.drop(columns=['_id'], inplace=True)
            mopj.rename(columns={'CLOSE': 'MOPJ'}, inplace=True)

            queryArgs = {'commodity': 'LL神华煤化工价格'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'price': 'L.DCE'}, inplace=True)

        queryArgs = {'wind_code': 'M0067855'}
        projectionField = ['date', 'CLOSE']
        records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exrate = pd.DataFrame.from_records(records, index='date')
        exrate.drop(columns=['_id'], inplace=True)
        exrate.rename(columns={'CLOSE': 'ExRate'}, inplace=True)

        total_df = ll.join(mopj, how='outer')
        total_df = total_df.join(exrate, how='left')  #这里需要注意汇率数据，不要使用outer方法

        # if method == 'future':
        # MOPJ外盘数据要滞后一天
        total_df['MOPJ'] = total_df['MOPJ'].shift(periods=1)
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['L.DCE'] - (total_df['MOPJ'] + 380) * 1.13 * 1.065 * total_df['ExRate'] - 150
        total_df['upper_profit_rate'] = total_df['upper_profit'] / ((total_df['MOPJ'] + 380) * 1.13 * 1.065 *
                                                                    total_df['ExRate'] + 150)

        total_df.drop(columns=['L.DCE', 'MOPJ', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'L.DCE'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'L.DCE', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入LL的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('LL的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('LL的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'L.DCE', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('LL的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('LL更新了%d条%s利润数据' % (count, method))

        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'L.DCE', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_pp_profit_rate(self, method='future'):
        """PP的利润公式：PP - 3 * MA - 800"""
        if method == 'future':
            queryArgs = {'wind_code': 'MA.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ma = pd.DataFrame.from_records(records, index='date')
            ma.drop(columns=['_id'], inplace=True)
            ma.rename(columns={'CLOSE': 'MA.CZC'}, inplace=True)

            queryArgs = {'wind_code': 'PP.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pp = pd.DataFrame.from_records(records, index='date')
            pp.drop(columns=['_id'], inplace=True)
            pp.rename(columns={'CLOSE': 'PP.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': '甲醇华东（江苏地区）'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ma = pd.DataFrame.from_records(records, index='date')
            ma.drop(columns=['_id'], inplace=True)
            ma.rename(columns={'price': 'MA.CZC'}, inplace=True)

            queryArgs = {'commodity': 'PP华东现货价'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pp = pd.DataFrame.from_records(records, index='date')
            pp.drop(columns=['_id'], inplace=True)
            pp.rename(columns={'price': 'PP.DCE'}, inplace=True)


        total_df = pp.join(ma, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['PP.DCE'] - 3 * total_df['MA.CZC'] - 800
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (3 * total_df['MA.CZC'] + 800)
        total_df.drop(columns=['PP.DCE', 'MA.CZC'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'PP.DCE'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'PP.DCE', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入PP的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('PP的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('PP的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'PP.DCE', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('PP的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('PP更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入PP的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'PP.DCE', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_ma_profit_rate(self, method='future'):
        """MA的利润公式：MA-(ZC+20)*1.95-600"""
        if method == 'future':
            queryArgs = {'wind_code': 'MA.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ma = pd.DataFrame.from_records(records, index='date')
            ma.drop(columns=['_id'], inplace=True)
            ma.rename(columns={'CLOSE': 'MA.CZC'}, inplace=True)

            queryArgs = {'wind_code': 'ZC.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            zc = pd.DataFrame.from_records(records, index='date')
            zc.drop(columns=['_id'], inplace=True)
            zc.rename(columns={'CLOSE': 'ZC.CZC'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': '甲醇华东（江苏地区）'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ma = pd.DataFrame.from_records(records, index='date')
            ma.drop(columns=['_id'], inplace=True)
            ma.rename(columns={'price': 'MA.CZC'}, inplace=True)

            queryArgs = {'edb_name': '秦皇岛港:平仓价:动力末煤(Q5500):山西产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            zc = pd.DataFrame.from_records(records, index='date')
            zc.drop(columns=['_id'], inplace=True)
            zc.rename(columns={'CLOSE': 'ZC.CZC'}, inplace=True)


        total_df = zc.join(ma, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['MA.CZC'] - (total_df['ZC.CZC'] + 20) * 1.95 - 600
        total_df['upper_profit_rate'] = total_df['upper_profit'] / ((total_df['ZC.CZC'] + 20) * 1.95 + 600)
        total_df.drop(columns=['ZC.CZC', 'MA.CZC'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'MA.CZC'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'MA.CZC', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入MA的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('MA的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('MA的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'MA.CZC', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('MA的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('MA更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入MA的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'MA.CZC', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_meg_profit_rate(self, method='future'):
        """MEG的利润公式：MEG-4*ZC-2400"""
        if method == 'future':
            queryArgs = {'wind_code': 'EG.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            meg = pd.DataFrame.from_records(records, index='date')
            meg.drop(columns=['_id'], inplace=True)
            meg.rename(columns={'CLOSE': 'EG.DCE'}, inplace=True)

            queryArgs = {'wind_code': 'ZC.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            zc = pd.DataFrame.from_records(records, index='date')
            zc.drop(columns=['_id'], inplace=True)
            zc.rename(columns={'CLOSE': 'ZC.CZC'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': 'MEG'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            meg = pd.DataFrame.from_records(records, index='date')
            meg.drop(columns=['_id'], inplace=True)
            meg.rename(columns={'price': 'EG.DCE'}, inplace=True)

            queryArgs = {'edb_name': '秦皇岛港:平仓价:动力末煤(Q5500):山西产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            zc = pd.DataFrame.from_records(records, index='date')
            zc.drop(columns=['_id'], inplace=True)
            zc.rename(columns={'CLOSE': 'ZC.CZC'}, inplace=True)


        total_df = zc.join(meg, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['EG.DCE'] - 4 * total_df['ZC.CZC'] - 2400
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (4 * total_df['ZC.CZC'] + 2400)
        total_df.drop(columns=['ZC.CZC', 'EG.DCE'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'EG.DCE'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'EG.DCE', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入EG的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('EG的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('EG的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'EG.DCE', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('EG的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('EG更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入MEG的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'EG.DCE', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_rb_profit_rate(self, method='future'):
        """RB的利润公式：RB - 1.7 * I - 0.5 * J - 800"""
        if method == 'future':
            queryArgs = {'wind_code': 'RB.SHF'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            rb = pd.DataFrame.from_records(records, index='date')
            rb.drop(columns=['_id'], inplace=True)
            rb.rename(columns={'CLOSE': 'RB.SHF'}, inplace=True)

            queryArgs = {'wind_code': 'I.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ii = pd.DataFrame.from_records(records, index='date')
            ii.drop(columns=['_id'], inplace=True)
            ii.rename(columns={'CLOSE': 'I.DCE'}, inplace=True)

            queryArgs = {'wind_code': 'J.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'edb_name': '价格:螺纹钢:HRB400 20mm:上海'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            rb = pd.DataFrame.from_records(records, index='date')
            rb.drop(columns=['_id'], inplace=True)
            rb.rename(columns={'CLOSE': 'RB.SHF'}, inplace=True)

            queryArgs = {'edb_name': '车板价:青岛港:澳大利亚:PB粉矿:61.5%'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ii = pd.DataFrame.from_records(records, index='date')
            ii.drop(columns=['_id'], inplace=True)
            ii.rename(columns={'CLOSE': 'I.DCE'}, inplace=True)

            queryArgs = {'edb_name': '天津港:平仓价(含税):一级冶金焦(A<12.5%,<0.65%S,CSR>65%,Mt8%):山西产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)


        total_df = rb.join(ii, how='outer')
        total_df = total_df.join(jj, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['RB.SHF'] - 1.7 * total_df['I.DCE'] - 0.5 * total_df['J.DCE'] - 800
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (1.7 * total_df['I.DCE'] + 0.5 * total_df['J.DCE'] + 800)
        total_df.drop(columns=['RB.SHF', 'I.DCE', 'J.DCE'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'RB.SHF'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'RB.SHF', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入RB的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('RB的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('RB的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'RB.SHF', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('RB的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('RB更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入RB的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'RB.SHF', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_hc_profit_rate(self, method='future'):
        """HC的利润公式：HC - 1.7 * I - 0.5 * J - 800"""
        if method == 'future':
            queryArgs = {'wind_code': 'HC.SHF'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            hc = pd.DataFrame.from_records(records, index='date')
            hc.drop(columns=['_id'], inplace=True)
            hc.rename(columns={'CLOSE': 'HC.SHF'}, inplace=True)

            queryArgs = {'wind_code': 'I.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ii = pd.DataFrame.from_records(records, index='date')
            ii.drop(columns=['_id'], inplace=True)
            ii.rename(columns={'CLOSE': 'I.DCE'}, inplace=True)

            queryArgs = {'wind_code': 'J.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'edb_name': '价格:热轧板卷:Q235B:4.75mm:杭州'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            hc = pd.DataFrame.from_records(records, index='date')
            hc.drop(columns=['_id'], inplace=True)
            hc.rename(columns={'CLOSE': 'HC.SHF'}, inplace=True)

            queryArgs = {'edb_name': '车板价:青岛港:澳大利亚:PB粉矿:61.5%'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ii = pd.DataFrame.from_records(records, index='date')
            ii.drop(columns=['_id'], inplace=True)
            ii.rename(columns={'CLOSE': 'I.DCE'}, inplace=True)

            queryArgs = {'edb_name': '天津港:平仓价(含税):一级冶金焦(A<12.5%,<0.65%S,CSR>65%,Mt8%):山西产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)


        total_df = hc.join(ii, how='outer')
        total_df = total_df.join(jj, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['HC.SHF'] - 1.7 * total_df['I.DCE'] - 0.5 * total_df['J.DCE'] - 800
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (1.7 * total_df['I.DCE'] + 0.5 * total_df['J.DCE'] + 800)
        total_df.drop(columns=['HC.SHF', 'I.DCE', 'J.DCE'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'HC.SHF'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'HC.SHF', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入HC的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('HC的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('HC的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'HC.SHF', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('HC的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('HC更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入HC的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'HC.SHF', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_j_profit_rate(self, method='future'):
        """J的利润公式：J - 1.2 * JM - 50"""
        if method == 'future':
            queryArgs = {'wind_code': 'JM.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jm = pd.DataFrame.from_records(records, index='date')
            jm.drop(columns=['_id'], inplace=True)
            jm.rename(columns={'CLOSE': 'JM.DCE'}, inplace=True)

            queryArgs = {'wind_code': 'J.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'edb_name': '天津港:库提价(含税):主焦煤(A<8%,V28%,0.8%S,G95,Y20mm):澳大利亚产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jm = pd.DataFrame.from_records(records, index='date')
            jm.drop(columns=['_id'], inplace=True)
            jm.rename(columns={'CLOSE': 'JM.DCE'}, inplace=True)

            queryArgs = {'edb_name': '天津港:平仓价(含税):一级冶金焦(A<12.5%,<0.65%S,CSR>65%,Mt8%):山西产'}
            projectionField = ['date', 'CLOSE']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            jj = pd.DataFrame.from_records(records, index='date')
            jj.drop(columns=['_id'], inplace=True)
            jj.rename(columns={'CLOSE': 'J.DCE'}, inplace=True)


        total_df = jm.join(jj, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['J.DCE'] - 1.2 * total_df['JM.DCE'] - 50
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (1.2 * total_df['JM.DCE'] + 50)
        total_df.drop(columns=['JM.DCE', 'J.DCE'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'J.DCE'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'J.DCE', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入J的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('J的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('J的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'J.DCE', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('J的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('J更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入J的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'J.DCE', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def get_ru_profit_rate(self):
        file_address = 'profit_db/ru.csv'
        df = pd.read_csv(file_address, index_col='时间', parse_dates=True)
        df.dropna(how='all', inplace=True)
        df.rename(columns={'橡胶利润(美元/吨)': 'upper_profit',
                           '橡胶利润率': 'upper_profit_rate'}, inplace=True)

        df['date'] = df.index
        df['commodity'] = 'RU.SHF'

        res_dict = df.to_dict(orient='index')
        print('写入RU的利润率')
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'RU.SHF', 'date': v['date']}
            projectionField = ['upper_profit', 'upper_profit_rate']
            res = self.target_coll.find_one(queryArgs, projectionField)
            if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
                count += 1
                continue
            elif res and (
                    res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
                self.target_coll.delete_many(queryArgs)
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            else:
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

    def calc_ru_profit_rate(self, method='future'):
        """RU的利润公式：(【TSR20合约结算价(美分/kg)】/100-(【杯胶（泰铢/kg)】+8.5)/【美元兑泰铢】)*1000"""
        if method == 'spot':
            # 橡胶价格只能更新到前一天，美元兑泰铢也是只能更新到前一天
            queryArgs = {'wind_code': 'S5016928'}
            projectionField = ['date', 'CLOSE']
            records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ru = pd.DataFrame.from_records(records, index='date')
            ru.drop(columns='_id', inplace=True)
            ru.rename(columns={'CLOSE': 'RU.SHF'}, inplace=True)

            queryArgs = {'wind_code': 'G0522656'}
            projectionField = ['date', 'CLOSE']
            records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            exrate = pd.DataFrame.from_records(records, index='date')
            exrate.drop(columns=['_id'], inplace=True)
            exrate.rename(columns={'CLOSE': 'ExRate'}, inplace=True)
        elif method == 'future':
            queryArgs = {'wind_code': 'RU.SHF'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ru = pd.DataFrame.from_records(records, index='date')
            ru.drop(columns=['_id'], inplace=True)
            ru.rename(columns={'CLOSE': 'RU.SHF'}, inplace=True)

            queryArgs = {'wind_code': 'M0330260'}
            projectionField = ['date', 'CLOSE']
            records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            exrate = pd.DataFrame.from_records(records, index='date')
            exrate.drop(columns=['_id'], inplace=True)
            exrate.rename(columns={'CLOSE': 'ExRate'}, inplace=True)

        queryArgs = {'commodity': '杯胶（泰铢/kg)'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        beijiao = pd.DataFrame.from_records(records, index='date')
        beijiao.drop(columns='_id', inplace=True)
        beijiao.rename(columns={'price': 'BEIJIAO'}, inplace=True)

        total_df = ru.join(beijiao, how='outer')
        total_df = total_df.join(exrate, how='left')

        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        if method == 'spot':
            total_df['upper_profit'] = (total_df['RU.SHF'] / 100. - (total_df['BEIJIAO'] + 8.5) / total_df[
                'ExRate']) * 1000.
        elif method == 'future':
            total_df['upper_profit'] = total_df['RU.SHF'] - (total_df['BEIJIAO'] + 8.5) / total_df['ExRate'] * 1000

        total_df['upper_profit_rate'] = total_df['upper_profit'] / (
                    (total_df['BEIJIAO'] + 8.5) / total_df['ExRate'] * 1000.)

        total_df.drop(columns=['RU.SHF', 'BEIJIAO', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'RU.SHF'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'RU.SHF', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入RU的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('RU的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('RU的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'RU.SHF', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('RU的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('RU更新了%d条%s利润数据' % (count, method))


        # res_dict = total_df.to_dict(orient='index')
        # print('插入RU的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'RU.SHF', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()


    def get_pvc_profit_rate(self):
        file_address = 'profit_db/pvc.csv'
        df = pd.read_csv(file_address, index_col='日期', parse_dates=True)
        df.dropna(how='all', inplace=True)
        df.rename(columns={'华北电石法单一利润': 'upper_profit',
                           '华北电石法单一利润率': 'upper_profit_rate'}, inplace=True)
        df['date'] = df.index
        df['commodity'] = 'V.DCE'

        res_dict = df.to_dict(orient='index')
        print('写入PVC的利润率')
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'V.DCE', 'date': v['date']}
            projectionField = ['upper_profit', 'upper_profit_rate']
            res = self.target_coll.find_one(queryArgs, projectionField)
            if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
                count += 1
                continue
            elif res and (
                    res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
                self.target_coll.delete_many(queryArgs)
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            else:
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

    def calc_pvc_profit_rate(self, method='future'):
        '''PVC的利润公式：【华北电石法 SG5】-(【电石华北 山东】*1.5+【液氯华北】*0.8+【华北电价】*0.5+1100)'''
        if method == 'spot':
            queryArgs = {'commodity': '华北电石法 SG5'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pvc = pd.DataFrame.from_records(records, index='date')
            pvc.drop(columns='_id', inplace=True)
            pvc.rename(columns={'price': 'V.DCE'}, inplace=True)
        elif method == 'future':
            queryArgs = {'wind_code': 'V.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pvc = pd.DataFrame.from_records(records, index='date')
            pvc.drop(columns='_id', inplace=True)
            pvc.rename(columns={'CLOSE': 'V.DCE'}, inplace=True)

        queryArgs = {'commodity': '电石华北 山东'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        dianshi = pd.DataFrame.from_records(records, index='date')
        dianshi.drop(columns='_id', inplace=True)
        dianshi.rename(columns={'price': 'DIANSHI'}, inplace=True)

        queryArgs = {'commodity': '液氯华北'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        yelv = pd.DataFrame.from_records(records, index='date')
        yelv.drop(columns='_id', inplace=True)
        yelv.rename(columns={'price': 'YELV'}, inplace=True)

        queryArgs = {'commodity': '华北电价'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        dianjia = pd.DataFrame.from_records(records, index='date')
        dianjia.drop(columns='_id', inplace=True)
        dianjia.rename(columns={'price': 'DIANJIA'}, inplace=True)


        total_df = pvc.join(dianshi, how='outer')
        total_df = total_df.join(yelv, how='outer')
        total_df = total_df.join(dianjia, how='outer')

        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['V.DCE'] - (
                    total_df['DIANSHI'] * 1.5 + total_df['YELV'] * 0.8 + total_df['DIANJIA'] * 0.5 + 1100)

        total_df['upper_profit_rate'] = total_df['upper_profit'] / (
                    total_df['DIANSHI'] * 1.5 + total_df['YELV'] * 0.8 + total_df['DIANJIA'] * 0.5 + 1100)

        total_df.drop(columns=['V.DCE', 'DIANSHI', 'YELV', 'DIANJIA'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'V.DCE'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'V.DCE', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入V的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('V的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('V的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'V.DCE', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('V的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('V更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入PVC的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'V.DCE', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_bu_profit_rate(self, method='future'):
        """BU的利润公式：BU - 7.5 * DUB-1M * 美元兑人民币"""
        if method == 'future':
            queryArgs = {'wind_code': 'BU.SHF'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            bu = pd.DataFrame.from_records(records, index='date')
            bu.drop(columns=['_id'], inplace=True)
            bu.rename(columns={'CLOSE': 'BU.SHF'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': '国产重交-山东'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            bu = pd.DataFrame.from_records(records, index='date')
            bu.drop(columns=['_id'], inplace=True)
            bu.rename(columns={'price': 'BU.SHF'}, inplace=True)

        queryArgs = {'tr_code': 'DUB-1M'}
        projectionField = ['date', 'CLOSE']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        dub = pd.DataFrame.from_records(records, index='date')
        dub.drop(columns=['_id'], inplace=True)
        dub.rename(columns={'CLOSE': 'DUB-1M'}, inplace=True)

        queryArgs = {'wind_code': 'M0067855'}
        projectionField = ['date', 'CLOSE']
        records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exrate = pd.DataFrame.from_records(records, index='date')
        exrate.drop(columns=['_id'], inplace=True)
        exrate.rename(columns={'CLOSE': 'ExRate'}, inplace=True)

        total_df = bu.join(dub, how='outer')
        total_df = total_df.join(exrate, how='left')
        # if method == 'future':
        total_df['DUB-1M'] = total_df['DUB-1M'].shift(periods=1)

        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['BU.SHF'] - 7.5 * total_df['DUB-1M'] * total_df['ExRate']
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (7.5 * total_df['DUB-1M'] * total_df['ExRate'])
        total_df.drop(columns=['BU.SHF', 'DUB-1M', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'BU.SHF'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'BU.SHF', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入BU的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('BU的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('BU的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'BU.SHF', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('BU的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('BU更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入BU的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'BU.SHF', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_pta_profit_rate(self, method='future'):
        """PTA的利润公式：PTA - (PX * 1.02 * 1.17 * 0.656 * 美元兑人民币)"""
        if method == 'future':
            queryArgs = {'wind_code': 'TA.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ta = pd.DataFrame.from_records(records, index='date')
            ta.drop(columns=['_id'], inplace=True)
            ta.rename(columns={'CLOSE': 'TA.CZC'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': 'TA内盘人民币价'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ta = pd.DataFrame.from_records(records, index='date')
            ta.drop(columns=['_id'], inplace=True)
            ta.rename(columns={'price': 'TA.CZC'}, inplace=True)

        queryArgs = {'commodity': 'PX'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        px = pd.DataFrame.from_records(records, index='date')
        px.drop(columns=['_id'], inplace=True)
        px.rename(columns={'price': 'PX'}, inplace=True)

        queryArgs = {'wind_code': 'M0067855'}
        projectionField = ['date', 'CLOSE']
        records = self.edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exrate = pd.DataFrame.from_records(records, index='date')
        exrate.drop(columns=['_id'], inplace=True)
        exrate.rename(columns={'CLOSE': 'ExRate'}, inplace=True)

        total_df = ta.join(px, how='outer')
        total_df = total_df.join(exrate, how='left')
        # if method == 'future':
        # total_df['PX'] = total_df['PX'].shift(periods=1)
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['TA.CZC'] - total_df['PX'] * 1.02 * 1.17 * 0.656 * total_df['ExRate']
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (total_df['PX'] * 1.02 * 1.17 * 0.656 *
                                                                    total_df['ExRate'])
        total_df.drop(columns=['TA.CZC', 'PX', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'TA.CZC'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'TA.CZC', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'upper_profit', 'upper_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['upper_profit', 'upper_profit_rate']] - exists_df[['upper_profit', 'upper_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入TA的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('TA的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('TA的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: upper_profit %f, upper_profit_rate %f, 现在修改为新值： upper_profit %f, upper_profit_rate %f' % (
                            exists_df.loc[i, 'upper_profit'], exists_df.loc[i, 'upper_profit_rate'],
                            total_df.loc[i, 'upper_profit'], total_df.loc[i, 'upper_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'TA.CZC', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('TA的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('TA更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入PTA的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'TA.CZC', 'date': v['date'], 'method': method}
        #     projectionField = ['upper_profit', 'upper_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_dimo_profit_rate(self, method='future'):
        '''地膜利润=地膜-LL神华*0.935-600'''
        queryArgs = {'commodity': '地膜'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        dimo = pd.DataFrame.from_records(records, index='date')
        dimo.drop(columns=['_id'], inplace=True)
        dimo.rename(columns={'price': 'DIMO'}, inplace=True)

        if method == 'future':
            queryArgs = {'wind_code': 'L.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'CLOSE': 'L.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': 'LL神华煤化工价格'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'price': 'L.DCE'}, inplace=True)
        total_df = dimo.join(ll, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)
        total_df['dimo_profit'] = total_df['DIMO'] - total_df['L.DCE'] * 0.935 - 600.
        total_df['dimo_profit_rate'] = total_df['dimo_profit'] / (total_df['L.DCE'] * 0.935 + 600)
        total_df.drop(columns=['L.DCE', 'DIMO'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'DIMO'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'DIMO', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'dimo_profit', 'dimo_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['dimo_profit', 'dimo_profit_rate']] - exists_df[['dimo_profit', 'dimo_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入地膜的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('地膜的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('地膜的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: dimo_profit %f, dimo_profit_rate %f, 现在修改为新值： dimo_profit %f, dimo_profit_rate %f' % (
                            exists_df.loc[i, 'dimo_profit'], exists_df.loc[i, 'dimo_profit_rate'],
                            total_df.loc[i, 'dimo_profit'], total_df.loc[i, 'dimo_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'DIMO', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('地膜的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('地膜更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入地膜的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'DIMO', 'date': v['date'], 'method': method}
        #     projectionField = ['dimo_profit', 'dimo_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['dimo_profit'] == v['dimo_profit'] and res['dimo_profit_rate'] == v['dimo_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (
        #             res['dimo_profit'] != v['dimo_profit'] or res['dimo_profit_rate'] != v['dimo_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_shuangfang_profit_rate(self, method='future'):
        '''双防膜利润=双防膜-(LL天津9085*0.6+华北重包*0.4)*0.915-1200'''
        queryArgs = {'commodity': '双防膜'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        shuangfang = pd.DataFrame.from_records(records, index='date')
        shuangfang.drop(columns=['_id'], inplace=True)
        shuangfang.rename(columns={'price': 'SHUANGFANG'}, inplace=True)

        queryArgs = {'commodity': '华北重包'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        zhongbao = pd.DataFrame.from_records(records, index='date')
        zhongbao.drop(columns=['_id'], inplace=True)
        zhongbao.rename(columns={'price': 'ZHONGBAO'}, inplace=True)

        if method == 'future':
            queryArgs = {'wind_code': 'L.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'CLOSE': 'L.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': '华北融指1线性（天津9085）'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'price': 'L.DCE'}, inplace=True)
        total_df = shuangfang.join(ll, how='outer')
        total_df = total_df.join(zhongbao, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)
        total_df['shuangfang_profit'] = total_df['SHUANGFANG'] - (
                    total_df['L.DCE'] * 0.6 + total_df['ZHONGBAO'] * 0.4) * 0.915 - 1200.
        total_df['shuangfang_profit_rate'] = total_df['shuangfang_profit'] / ((
                    total_df['L.DCE'] * 0.6 + total_df['ZHONGBAO'] * 0.4) * 0.915 + 1200.)
        total_df.drop(columns=['L.DCE', 'SHUANGFANG', 'ZHONGBAO'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'SHUANGFANG'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'SHUANGFANG', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'shuangfang_profit', 'shuangfang_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['shuangfang_profit', 'shuangfang_profit_rate']] - exists_df[['shuangfang_profit', 'shuangfang_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入双防膜的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('双防膜的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('双防膜的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: shuangfang_profit %f, shuangfang_profit_rate %f, 现在修改为新值： shuangfang_profit %f, shuangfang_profit_rate %f' % (
                            exists_df.loc[i, 'shuangfang_profit'], exists_df.loc[i, 'shuangfang_profit_rate'],
                            total_df.loc[i, 'shuangfang_profit'], total_df.loc[i, 'shuangfang_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'SHUANGFANG', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('双防膜的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('双防膜更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入双防膜的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'SHUANGFANG', 'date': v['date'], 'method': method}
        #     projectionField = ['shuangfang_profit', 'shuangfang_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['shuangfang_profit'] == v['shuangfang_profit'] and res['shuangfang_profit_rate'] == v[
        #         'shuangfang_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['shuangfang_profit'] != v['shuangfang_profit'] or res['shuangfang_profit_rate'] != v[
        #         'shuangfang_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_chanrao_profit_rate(self, method='future'):
        '''缠绕膜利润=缠绕膜-LL华东-1500'''
        queryArgs = {'commodity': '缠绕膜'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        chanrao = pd.DataFrame.from_records(records, index='date')
        chanrao.drop(columns=['_id'], inplace=True)
        chanrao.rename(columns={'price': 'CHANRAO'}, inplace=True)

        if method == 'future':
            queryArgs = {'wind_code': 'L.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'CLOSE': 'L.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': 'LL华东'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            ll = pd.DataFrame.from_records(records, index='date')
            ll.drop(columns=['_id'], inplace=True)
            ll.rename(columns={'price': 'L.DCE'}, inplace=True)
        total_df = chanrao.join(ll, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)
        total_df['chanrao_profit'] = total_df['CHANRAO'] - total_df['L.DCE'] - 1500.
        total_df['chanrao_profit_rate'] = total_df['chanrao_profit'] / (total_df['L.DCE'] + 1500.)
        total_df.drop(columns=['L.DCE', 'CHANRAO'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'CHANRAO'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'CHANRAO', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'chanrao_profit', 'chanrao_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['chanrao_profit', 'chanrao_profit_rate']] - exists_df[['chanrao_profit', 'chanrao_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入缠绕膜的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('缠绕膜的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('缠绕膜的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: chanrao_profit %f, chanrao_profit_rate %f, 现在修改为新值： chanrao_profit %f, chanrao_profit_rate %f' % (
                            exists_df.loc[i, 'chanrao_profit'], exists_df.loc[i, 'chanrao_profit_rate'],
                            total_df.loc[i, 'chanrao_profit'], total_df.loc[i, 'chanrao_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'CHANRAO', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('缠绕膜的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('缠绕膜更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入缠绕膜的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'CHANRAO', 'date': v['date'], 'method': method}
        #     projectionField = ['chanrao_profit', 'chanrao_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['chanrao_profit'] == v['chanrao_profit'] and res['chanrao_profit_rate'] == v[
        #         'chanrao_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['chanrao_profit'] != v['chanrao_profit'] or res['chanrao_profit_rate'] != v[
        #         'chanrao_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_bopp_profit_rate(self, method='future'):
        '''BOPP利润=BOPP-PP-1500'''
        queryArgs = {'commodity': 'BOPP膜'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        bopp = pd.DataFrame.from_records(records, index='date')
        bopp.drop(columns=['_id'], inplace=True)
        bopp.rename(columns={'price': 'BOPP'}, inplace=True)

        if method == 'future':
            queryArgs = {'wind_code': 'PP.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pp = pd.DataFrame.from_records(records, index='date')
            pp.drop(columns=['_id'], inplace=True)
            pp.rename(columns={'CLOSE': 'PP.DCE'}, inplace=True)
        elif method == 'spot':
            queryArgs = {'commodity': 'PP华东现货价'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pp = pd.DataFrame.from_records(records, index='date')
            pp.drop(columns=['_id'], inplace=True)
            pp.rename(columns={'price': 'PP.DCE'}, inplace=True)
        total_df = bopp.join(pp, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)
        total_df['bopp_profit'] = total_df['BOPP'] - total_df['PP.DCE'] - 1500.
        total_df['bopp_profit_rate'] = total_df['bopp_profit'] / (total_df['PP.DCE'] + 1500.)
        total_df.drop(columns=['PP.DCE', 'BOPP'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'BOPP'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'BOPP', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'bopp_profit', 'bopp_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['bopp_profit', 'bopp_profit_rate']] - exists_df[['bopp_profit', 'bopp_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入BOPP的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('BOPP的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('BOPP的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: bopp_profit %f, bopp_profit_rate %f, 现在修改为新值： bopp_profit %f, bopp_profit_rate %f' % (
                            exists_df.loc[i, 'bopp_profit'], exists_df.loc[i, 'bopp_profit_rate'],
                            total_df.loc[i, 'bopp_profit'], total_df.loc[i, 'bopp_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'BOPP', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('BOPP的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('BOPP更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入BOPP的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'BOPP', 'date': v['date'], 'method': method}
        #     projectionField = ['bopp_profit', 'bopp_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['bopp_profit'] == v['bopp_profit'] and res['bopp_profit_rate'] == v['bopp_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['bopp_profit'] != v['bopp_profit'] or res['bopp_profit_rate'] != v['bopp_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def calc_poy_profit_rate(self, method='future'):
        '''POY利润 = POY-（0.855*PTA+0.335*MEG）-1150'''
        queryArgs = {'commodity': 'POY150D/48F'}
        projectionField = ['date', 'price']
        records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        poy = pd.DataFrame.from_records(records, index='date')
        poy.drop(columns=['_id'], inplace=True)
        poy.rename(columns={'price': 'POY'}, inplace=True)

        if method == 'future':
            queryArgs = {'wind_code': 'TA.CZC'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pta = pd.DataFrame.from_records(records, index='date')
            pta.drop(columns=['_id'], inplace=True)
            pta.rename(columns={'CLOSE': 'TA.CZC'}, inplace=True)

            queryArgs = {'wind_code': 'EG.DCE'}
            projectionField = ['date', 'CLOSE']
            records = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            meg = pd.DataFrame.from_records(records, index='date')
            meg.drop(columns=['_id'], inplace=True)
            meg.rename(columns={'CLOSE': 'EG.DCE'}, inplace=True)

        elif method == 'spot':
            queryArgs = {'commodity': 'TA内盘人民币价'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            pta = pd.DataFrame.from_records(records, index='date')
            pta.drop(columns=['_id'], inplace=True)
            pta.rename(columns={'price': 'TA.CZC'}, inplace=True)

            queryArgs = {'commodity': 'MEG'}
            projectionField = ['date', 'price']
            records = self.spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            meg = pd.DataFrame.from_records(records, index='date')
            meg.drop(columns=['_id'], inplace=True)
            meg.rename(columns={'price': 'EG.DCE'}, inplace=True)

        total_df = poy.join(pta, how='outer')
        total_df = total_df.join(meg, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)
        total_df['poy_profit'] = total_df['POY'] - (total_df['TA.CZC'] * 0.855 + total_df['EG.DCE'] * 0.335) - 1150.
        total_df['poy_profit_rate'] = total_df['poy_profit'] / (
                    total_df['TA.CZC'] * 0.855 + total_df['EG.DCE'] * 0.335 + 1150.)
        total_df.drop(columns=['EG.DCE', 'POY', 'TA.CZC'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'POY'
        total_df['method'] = method

        start_date = total_df.index[0]

        queryArgs = {'commodity': 'POY', 'date': {'$gte': start_date}, 'method': method}
        projectionField = ['date', 'commodity', 'poy_profit', 'poy_profit_rate', 'method']
        res = self.target_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        exists_df = pd.DataFrame.from_records(res, index='date')
        exists_df.drop(columns='_id', inplace=True)
        sub_res = total_df[['poy_profit', 'poy_profit_rate']] - exists_df[['poy_profit', 'poy_profit_rate']]
        isZero = (sub_res == 0).all(axis=1).values
        diff_res = sub_res.loc[~isZero]

        self.logger.info('插入POY的%s利润率' % method)
        if diff_res.empty:
            self.logger.info('POY的%s利润率没有新的数据' % method)
        else:
            count = 0
            for i in diff_res.index:
                if i not in total_df.index:
                    continue
                elif i in exists_df.index:
                    # 说明新计算的数据与已存在的数据不一致
                    self.logger.info('POY的%s利润率在%s这一天的数据与数据库存在的数据不一致' % (method, i))
                    self.logger.info(
                        '数据库之前存在的数据: poy_profit %f, poy_profit_rate %f, 现在修改为新值： poy_profit %f, poy_profit_rate %f' % (
                            exists_df.loc[i, 'poy_profit'], exists_df.loc[i, 'poy_profit_rate'],
                            total_df.loc[i, 'poy_profit'], total_df.loc[i, 'poy_profit_rate']))
                    self.target_coll.delete_many({'commodity': 'POY', 'date': i, 'method': method})
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数据是新增的数据
                    self.logger.info('POY的%s利润率在%s这一天的数据进入数据库' % (method, i))
                    res_dict = total_df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    self.target_coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('POY更新了%d条%s利润数据' % (count, method))

        # res_dict = total_df.to_dict(orient='index')
        # print('插入POY的%s利润率' % method)
        # total = len(res_dict)
        # count = 1
        # for k, v in res_dict.items():
        #
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     queryArgs = {'commodity': 'POY', 'date': v['date'], 'method': method}
        #     projectionField = ['poy_profit', 'poy_profit_rate']
        #     res = self.target_coll.find_one(queryArgs, projectionField)
        #     if res and res['poy_profit'] == v['poy_profit'] and res['poy_profit_rate'] == v['poy_profit_rate']:
        #         count += 1
        #         continue
        #     elif res and (res['poy_profit'] != v['poy_profit'] or res['poy_profit_rate'] != v['poy_profit_rate']):
        #         self.target_coll.delete_many(queryArgs)
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     else:
        #         v.update({'update_time': datetime.now()})
        #         self.target_coll.insert_one(v)
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

a = ProfitRate()
a.calc_ll_profit_rate(method='future')
a.calc_ll_profit_rate(method='spot')
a.calc_pp_profit_rate(method='future')
a.calc_pp_profit_rate(method='spot')
a.calc_ma_profit_rate(method='future')
a.calc_ma_profit_rate(method='spot')
a.calc_meg_profit_rate(method='future')
a.calc_meg_profit_rate(method='spot')
a.calc_rb_profit_rate(method='future')
a.calc_rb_profit_rate(method='spot')
a.calc_hc_profit_rate(method='future')
a.calc_hc_profit_rate(method='spot')
a.calc_j_profit_rate(method='future')
a.calc_j_profit_rate(method='spot')
a.calc_ru_profit_rate(method='future')
a.calc_ru_profit_rate(method='spot')
a.calc_pvc_profit_rate(method='future')
a.calc_pvc_profit_rate(method='spot')
a.calc_bu_profit_rate(method='future')
a.calc_bu_profit_rate(method='spot')
a.calc_pta_profit_rate(method='future')
a.calc_pta_profit_rate(method='spot')
a.calc_dimo_profit_rate(method='future')
a.calc_dimo_profit_rate(method='spot')
a.calc_shuangfang_profit_rate(method='future')
a.calc_shuangfang_profit_rate(method='spot')
a.calc_chanrao_profit_rate(method='future')
a.calc_chanrao_profit_rate(method='spot')
a.calc_bopp_profit_rate(method='future')
a.calc_bopp_profit_rate(method='spot')
a.calc_poy_profit_rate(method='future')
a.calc_poy_profit_rate(method='spot')