"""
根据基本面的研究生成各品种的利润率数据，并入库
"""

import pymongo
import pandas as pd
from datetime import datetime
import sys


class ProfitRate(object):

    def __init__(self):
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
        total_df = total_df.join(exrate, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['L.DCE'] - (total_df['MOPJ'] + 380) * 1.13 * 1.065 * total_df['ExRate'] - 150
        total_df['upper_profit_rate'] = total_df['upper_profit'] / ((total_df['MOPJ'] + 380) * 1.13 * 1.065 *
                                                                    total_df['ExRate'] + 150)
        total_df.drop(columns=['L.DCE', 'MOPJ', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'L.DCE'
        total_df['method'] = method

        res_dict = total_df.to_dict(orient='index')
        print('插入LL的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():

            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'L.DCE', 'date': v['date'], 'method': method}
            projectionField = ['upper_profit', 'upper_profit_rate']
            res = self.target_coll.find_one(queryArgs, projectionField)
            if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
                count += 1
                continue
            elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
                self.target_coll.delete_many(queryArgs)
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            else:
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

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

        res_dict = total_df.to_dict(orient='index')
        print('插入PP的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():

            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'PP.DCE', 'date': v['date'], 'method': method}
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

        res_dict = total_df.to_dict(orient='index')
        print('插入MA的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'MA.CZC', 'date': v['date'], 'method': method}
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

        res_dict = total_df.to_dict(orient='index')
        print('插入MEG的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'EG.DCE', 'date': v['date'], 'method': method}
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

        res_dict = total_df.to_dict(orient='index')
        print('插入RB的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'RB.SHF', 'date': v['date'], 'method': method}
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

        res_dict = total_df.to_dict(orient='index')
        print('插入HC的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'HC.SHF', 'date': v['date'], 'method': method}
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

        res_dict = total_df.to_dict(orient='index')
        print('插入J的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'J.DCE', 'date': v['date'], 'method': method}
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
            queryArgs = {'commodity': '国产重交-长三角'}
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
        total_df = total_df.join(exrate, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['BU.SHF'] - 7.5 * total_df['DUB-1M'] * total_df['ExRate']
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (7.5 * total_df['DUB-1M'] * total_df['ExRate'])
        total_df.drop(columns=['BU.SHF', 'DUB-1M', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'BU.SHF'
        total_df['method'] = method

        res_dict = total_df.to_dict(orient='index')
        print('插入BU的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():

            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'BU.SHF', 'date': v['date'], 'method': method}
            projectionField = ['upper_profit', 'upper_profit_rate']
            res = self.target_coll.find_one(queryArgs, projectionField)
            if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
                count += 1
                continue
            elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
                self.target_coll.delete_many(queryArgs)
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            else:
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

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
        total_df = total_df.join(exrate, how='outer')
        total_df.fillna(method='ffill', inplace=True)
        total_df.dropna(inplace=True)

        total_df['upper_profit'] = total_df['TA.CZC'] - total_df['PX'] * 1.02 * 1.17 * 0.656 * total_df['ExRate']
        total_df['upper_profit_rate'] = total_df['upper_profit'] / (total_df['PX'] * 1.02 * 1.17 * 0.656 *
                                                                    total_df['ExRate'])
        total_df.drop(columns=['TA.CZC', 'PX', 'ExRate'], inplace=True)
        total_df['date'] = total_df.index
        total_df['commodity'] = 'TA.CZC'
        total_df['method'] = method

        res_dict = total_df.to_dict(orient='index')
        print('插入PTA的%s利润率' % method)
        total = len(res_dict)
        count = 1
        for k, v in res_dict.items():

            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            queryArgs = {'commodity': 'TA.CZC', 'date': v['date'], 'method': method}
            projectionField = ['upper_profit', 'upper_profit_rate']
            res = self.target_coll.find_one(queryArgs, projectionField)
            if res and res['upper_profit'] == v['upper_profit'] and res['upper_profit_rate'] == v['upper_profit_rate']:
                count += 1
                continue
            elif res and (res['upper_profit'] != v['upper_profit'] or res['upper_profit_rate'] != v['upper_profit_rate']):
                self.target_coll.delete_many(queryArgs)
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            else:
                v.update({'update_time': datetime.now()})
                self.target_coll.insert_one(v)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

a = ProfitRate()
# a.calc_ll_profit_rate(method='future')
# a.calc_ll_profit_rate(method='spot')
# a.calc_pp_profit_rate(method='future')
# a.calc_pp_profit_rate(method='spot')
# a.calc_ma_profit_rate(method='future')
# a.calc_ma_profit_rate(method='spot')
# a.calc_meg_profit_rate(method='future')
# a.calc_meg_profit_rate(method='spot')
# a.calc_rb_profit_rate(method='future')
# a.calc_rb_profit_rate(method='spot')
# a.calc_hc_profit_rate(method='future')
# a.calc_hc_profit_rate(method='spot')
# a.calc_j_profit_rate(method='future')
# a.calc_j_profit_rate(method='spot')
# a.get_ru_profit_rate()
# a.get_pvc_profit_rate()
# a.calc_bu_profit_rate(method='future')
# a.calc_bu_profit_rate(method='spot')
a.calc_pta_profit_rate(method='future')
a.calc_pta_profit_rate(method='spot')

