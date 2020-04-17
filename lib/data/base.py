
import pymongo
from WindPy import w
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pprint
import sys
import re
import eikon as ek
import logging
import os


class DataSaving(object):
    def __init__(self, host, port, usr, pwd, db, log_path):

        # 数据库登录的设置
        self.mongo_login = False
        self.conn = None
        self.db = None

        self.host = host
        self.port = port
        self.usr = usr
        self.pwd = pwd
        self.db_name = db


        #  日志的设置
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(filename)s %(funcName)s %(levelname)s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S %a')

        fh = logging.FileHandler(log_path)
        ch = logging.StreamHandler()
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def mongoConn(self):
        '''MongoDB数据库的登录'''
        if not self.mongo_login:
            self.conn = pymongo.MongoClient(host=self.host, port=self.port)
            self.db = self.conn[self.db_name]
            self.db.authenticate(self.usr, self.pwd)
            self.mongo_login = True

    @staticmethod
    def rtConn():
        try:
            TR_ID = '70650e2c881040408f6f95dea2bf3fa13e9f66fe'
            ek.set_app_key(TR_ID)
        except ek.eikonError.EikonError as e:
            print(e)
        return None

    @staticmethod
    def windConn():
        if not w.isconnected():
            w.start()
        return None

    def getFuturesMinPriceFromWind(self, collection, ctr, frequency, save_mode='MongoDB', save_path=None, category=None,
                                   **kwargs):

        self.windConn()
        # 如果是存到数据库
        if save_mode in ['MongoDB', 'MongoDB CSV']:
            self.mongoConn()
            coll = self.db[collection]
            coll_info = self.db['Information']
            ptn = re.compile('\d+(?=min)')
            freq = ptn.search(frequency).group()
            queryArgs = {'wind_code': ctr, 'frequency': frequency}
            projectionField = ['wind_code', 'date_time']
            res = coll.find(queryArgs, projectionField).sort('date_time', pymongo.DESCENDING).limit(1)
            res = list(res)

            now_dttm = datetime.now()

            # 第一次出现
            if not res:
                queryArgs = {'wind_code': ctr}
                projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
                res_info = coll_info.find(queryArgs, projectionField)
                res_info = list(res_info)
                # 主力合约
                if not res_info:
                    start_time = datetime.today() - timedelta(days=1200)
                    start_time = start_time.replace(hour=9) - timedelta(minutes=1)

                    if 6 < now_dttm.hour < 16:
                        end_time = now_dttm.replace(hour=8, minute=0, second=0, microsecond=0)
                    elif now_dttm.hour < 6:
                        end_time = now_dttm.replace(day=now_dttm.day - 1, hour=16, minute=0, second=0, microsecond=0)
                    else:
                        end_time = now_dttm.replace(hour=16, minute=0, second=0, microsecond=0)


                # 具体单个合约
                else:
                    start_time = res_info[0]['contract_issue_date']
                    start_time = start_time.replace(hour=9) - timedelta(minutes=1)
                    last_trade_date = res_info[0]['last_trade_date']
                    last_trade_date.replace(hour=16) + timedelta(minutes=1)

                    if 6 < now_dttm.hour < 16:
                        end_time = min(now_dttm.replace(hour=8, minute=0, second=0, microsecond=0), last_trade_date)
                    elif now_dttm.hour < 6:
                        end_time = min(
                            now_dttm.replace(day=now_dttm.day - 1, hour=16, minute=0, second=0, microsecond=0),
                            last_trade_date)
                    else:
                        end_time = min(now_dttm.replace(hour=16, minute=0, second=0, microsecond=0), last_trade_date)



            # 不是第一次出现
            else:
                start_time = res[0]['date_time'] + timedelta(minutes=1)
                start_time = start_time.replace(hour=9) - timedelta(minutes=1)
                if 6 < now_dttm.hour < 16:
                    end_time = now_dttm.replace(hour=8, minute=0, second=0, microsecond=0)
                elif now_dttm.hour < 6:
                    end_time = now_dttm.replace(day=now_dttm.day - 1, hour=16, minute=0, second=0, microsecond=0)
                else:
                    end_time = now_dttm.replace(hour=16, minute=0, second=0, microsecond=0)


            if start_time > end_time:
                return
            res = w.wsi(ctr, "open,high,low,close,volume,amt,oi", beginTime=start_time, endTime=end_time)
            if res.ErrorCode == -40520007 or res.ErrorCode == -40520017:
                print(ctr, res.Data)
                return
            res_df = pd.DataFrame.from_dict(dict(zip(res.Fields, res.Data)))
            res_df.index = res.Times
            res_df.dropna(how='all', subset=['open', 'high', 'low', 'close'], inplace=True)

            res_df['wind_code'] = ctr
            res_df['frequency'] = frequency
            res_dict = res_df.to_dict(orient='index')
            total = len(res_dict)
            count = 1
            self.logger.info(
                u'抓取%s合约从%s到%s的%s分钟数据' % (ctr, start_time.strftime('%Y%m%d'), end_time.strftime('%Y%m%d'), freq))
            for di in res_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = res_dict[di].copy()
                dtemp['date_time'] = datetime.strptime(str(di), '%Y-%m-%d %H:%M:%S')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)
                coll.insert_one(dtemp)

                count += 1
            sys.stdout.write('\n')
            sys.stdout.flush()
        # 如果是存到CSV
        if save_mode in ['CSV', 'MongoDB CSV']:
            if not save_path:
                print(ctr)
                raise Exception('没有提供保存的CSV路径save_path字段')
            if not category:
                print(ctr)
                raise Exception('没有提供该合约的category字段')
            path_new = os.path.join(save_path, collection, category)
            if not os.path.exists(path_new):
                os.makedirs(path_new)
            file_path = os.path.join(path_new, '%s.csv' % ctr)
            # 第一次生成
            if not os.path.exists(file_path):
                self.mongoConn()
                coll_info = self.db['Information']
                queryArgs = {'wind_code': ctr}
                projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
                res_info = coll_info.find(queryArgs, projectionField)
                res_info = list(res_info)
                if not res_info:
                    start_time = datetime.today() - timedelta(days=1200)
                else:
                    start_time = res_info[0]['contract_issue_date']
                start_time = start_time.replace(hour=9) - timedelta(minutes=1)

                now_dttm = datetime.now()
                if 6 < now_dttm.hour < 16:
                    end_time = now_dttm.replace(hour=8, minute=0, second=0, microsecond=0)
                elif now_dttm.hour < 6:
                    end_time = now_dttm.replace(day=now_dttm.day - 1, hour=16, minute=0, second=0, microsecond=0)
                else:
                    end_time = now_dttm.replace(hour=16, minute=0, second=0, microsecond=0)

                if start_time > end_time:
                    return
                res = w.wsi(ctr, "open,high,low,close,volume,amt,oi", beginTime=start_time, endTime=end_time)
                if res.ErrorCode == -40520007 or res.ErrorCode == -40520017:
                    print(ctr, res.Data)
                    return
                res_df = pd.DataFrame.from_dict(dict(zip(res.Fields, res.Data)))
                res_df.index = res.Times
                res_df.index.name = 'date_time'
                res_df['date'] = [dt.strftime('%Y-%m-%d') for dt in res_df.index]
                res_df['time'] = [dt.strftime('%H:%M:%S') for dt in res_df.index]
                res_df.dropna(how='all', subset=['open', 'high', 'low', 'close'], inplace=True)
                self.logger.info('生成%s合约从%s到%s分钟数据的csv文件' % (ctr, res_df.index[0], res_df.index[-1]))
                res_df.to_csv(file_path)
            else:
                df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                start_time = df.index[-1] + timedelta(minutes=1)
                now_dttm = datetime.now()
                if 6 < now_dttm.hour < 16:
                    end_time = now_dttm.replace(hour=8, minute=0, second=0, microsecond=0)
                elif now_dttm.hour < 6:
                    end_time = now_dttm.replace(day=now_dttm.day - 1, hour=16, minute=0, second=0, microsecond=0)
                else:
                    end_time = now_dttm.replace(hour=16, minute=0, second=0, microsecond=0)

                if start_time > end_time:
                    return
                res = w.wsi(ctr, "open,high,low,close,volume,amt,oi", beginTime=start_time, endTime=end_time)
                if res.ErrorCode == -40520007 or res.ErrorCode == -40520017:
                    self.logger.info('%s合约出现了%sWIND错误' % (ctr, res.Data))
                    return
                elif res.ErrorCode == -40520008:
                    self.logger.info('万得登陆超时，请重新登陆')
                    return

                try:
                    res_df = pd.DataFrame.from_dict(dict(zip(res.Fields, res.Data)))
                    res_df.index = res.Times
                    res_df.index.name = 'date_time'
                    res_df['date'] = [dt.strftime('%Y-%m-%d') for dt in res_df.index]
                    res_df['time'] = [dt.strftime('%H:%M:%S') for dt in res_df.index]
                    res_df.dropna(how='all', subset=['open', 'high', 'low', 'close'], inplace=True)
                    if res_df.empty:
                        self.logger.info('在%s到%s的时间段内%s合约没有数据' % (start_time, end_time, ctr))
                        return
                    df = pd.concat((df, res_df))
                    self.logger.info('在csv文件中增加%s合约从%s到%s分钟数据' % (ctr, res_df.index[0], res_df.index[-1]))
                    df.to_csv(file_path)
                except Exception as e:
                    print(ctr)
                    print(res)
                    print(res_df)
                    raise Exception('%s出现问题。' % ctr + e.message)


    def getFuturesPriceFromTB(self, collection, ctr, path, frequency):
        coll = self.db[collection]
        file_path = os.path.join(path, '%s_%s.csv' % (ctr, frequency))
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        queryArgs = {'tb_code': ctr, 'frequency': frequency}
        if frequency == '1min':
            date_label = 'date_time'
        else:
            date_label = 'date'
        projectionField = [date_label]
        records = list(coll.find(queryArgs, projectionField).sort(date_label, pymongo.DESCENDING).limit(1))

        if records:
            dt_last = records[0][date_label]
            df = df[df.index > dt_last]
            if df.empty:
                return

        df[date_label] = df.index
        df['tb_code'] = ctr
        df['frequency'] = frequency

        df_dict = df.to_dict(orient='index')

        self.logger.info(
            '抓取%s从%s到%s的数据' % (ctr, df.index[0].strftime('%Y%m%d %H:%M:%S'), df.index[-1].strftime('%Y%m%d %H:%M:%S')))

        total = len(df_dict)
        count = 1
        for d in df_dict:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            dtemp = df_dict[d].copy()
            dtemp.update({'update_time': datetime.now()})
            coll.insert_one(dtemp)

            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

    def getFuturesOIRFromWind(self, collection, cmd, **kwargs):
        self.windConn()
        self.mongoConn()
        coll = self.db[collection]
        coll_info = self.db['Information']
        coll_finished = self.db['FinishedContracts']

        ptn1 = re.compile('[A-Z]+(?=\.)')
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd1 = ptn1.search(cmd).group()
        cmd2 = ptn2.search(cmd).group()

        # Information表里的所有已有的合约
        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        info_list = coll_info.find(queryArgs, projectionField)

        # FinishedContracts里有的关于多头、空头持仓和成交量排序的所有合约
        queryArgs_long = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}, 'collection': collection,
                          'type': 'long'}
        projectionField_long = ['wind_code', 'contract_issue_date', 'last_trade_date']
        inDB_list_long = coll_finished.find(queryArgs_long, projectionField_long)

        queryArgs_short = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}, 'collection': collection,
                           'type': 'short'}
        projectionField_short = ['wind_code', 'contract_issue_date', 'last_trade_date']
        inDB_list_short = coll_finished.find(queryArgs_short, projectionField_short)

        queryArgs_volume = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}, 'collection': collection,
                            'type': 'volume'}
        projectionField_volume = ['wind_code', 'contract_issue_date', 'last_trade_date']
        inDB_list_volume = coll_finished.find(queryArgs_volume, projectionField_volume)

        # 检查是否有新的合约上市或者是列出第一次导入该品种的数据时的合约
        info_list = [(c['wind_code'], c['contract_issue_date'], c['last_trade_date']) for c in info_list]

        inDB_list_long = [(c['wind_code'], c['contract_issue_date'], c['last_trade_date']) for c in inDB_list_long]
        new_list_long = list(set(info_list).difference(set(inDB_list_long)))
        new_list_long.sort(key=lambda x: x[-1], reverse=False)

        inDB_list_short = [(c['wind_code'], c['contract_issue_date'], c['last_trade_date']) for c in inDB_list_short]
        new_list_short = list(set(info_list).difference(set(inDB_list_short)))
        new_list_short.sort(key=lambda x: x[-1], reverse=False)


        total = len(new_list_long)
        count = 1
        print('针对%s品种的新合约或从未导入的合约进行持仓信息抓取' % cmd)
        for r in new_list_long:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()
            wind_code = r[0]
            issue_date = r[1]
            last_trade_date = r[2]

            queryArgs = {'wind_code': wind_code}
            projectionField = ['wind_code', 'date']
            dt_end_res = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            dt_start_res = coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING).limit(1)
            dt_end_res = list(dt_end_res)
            dt_start_res = list(dt_start_res)

            # 如果数据库里没有持仓数据

            if not dt_end_res and not dt_start_res:
                end_dt = min(last_trade_date, datetime.today())
                res = w.wset(tablename='openinterestranking', startdate=issue_date.strftime('%Y-%m-%d'),
                             enddate=end_dt.strftime('%Y-%m-%d'), varity=cmd, wind_code=wind_code,
                             order_by='long', ranks='all',
                             field='date,ranks,member_name,long_position,long_position_increase')

                if res.ErrorCode == -40522017:
                    raise Exception(u'数据提取量超限')

                # 如果没有数据，而且已经过了最后交易日
                if not res.Data and last_trade_date < datetime.today():
                    empty_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                  'last_trade_date': last_trade_date, 'collection': collection,
                                  'status': 'Null', 'update_time': datetime.now()}
                    coll_finished.insert_one(empty_dict)
                    count += 1
                    continue
                # 如果没有数据，而且没有过最后交易日
                elif not res.Data and last_trade_date >= datetime.today():
                    info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                 'last_trade_date': last_trade_date, 'collection': collection,
                                 'status': 'Unfinished', 'update_time': datetime.now()}
                    coll_finished.insert_one(info_dict)
                    count += 1
                    continue
                # 如果有数据
                else:
                    res_dict = dict(zip(res.Fields, res.Data))
                    df = pd.DataFrame.from_dict(res_dict)
                    df['wind_code'] = wind_code
                    df['commodity'] = cmd
                    df['type'] = 'long'
                    df2dict = df.to_dict(orient='index')
                    for di in df2dict:
                        dtemp = df2dict[di].copy()
                        dtemp['update_time'] = datetime.now()
                        dtemp.update(kwargs)
                        coll.insert_one(dtemp)
                    # 如果当前日期已经过了最后交易日
                    if last_trade_date < datetime.today():
                        info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                     'last_trade_date': last_trade_date, 'collection': collection,
                                     'status': 'Finished', 'update_time': datetime.now()}
                        coll_finished.insert_one(info_dict)
                        count += 1
                    # 如果没有过最后交易日
                    else:
                        info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                     'last_trade_date': last_trade_date, 'collection': collection,
                                     'status': 'Unfinished', 'update_time': datetime.now()}
                        coll_finished.insert_one(info_dict)
                        count += 1

            # 如果数据库里有数据，而且最后的数据没有过最后交易日
            elif dt_end_res[0]['date'] < last_trade_date:
                dt_start = dt_end_res[0]['date'] + timedelta(1)
                dt_end = min(last_trade_date, datetime.today())
                res = w.wset(tablename='openinterestranking', startdate=dt_start.strftime('%Y-%m-%d'),
                             enddate=dt_end.strftime('%Y-%m-%d'), varity=cmd, wind_code=wind_code, order_by='long',
                             ranks='all', field='date,ranks,member_name,long_position,long_position_increase')

                if res.ErrorCode == -40522017:
                    raise Exception(u'数据提取量超限')


                if not res.Data and last_trade_date < datetime.today():
                    info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                 'last_trade_date': last_trade_date, 'collection': collection,
                                 'status': 'Finished', 'update_time': datetime.now()}
                    coll_finished.insert_one(info_dict)
                    count += 1
                elif not res.Data and last_trade_date >= datetime.today():
                    info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                 'last_trade_date': last_trade_date, 'collection': collection,
                                 'status': 'Unfinished', 'update_time': datetime.now()}
                    coll_finished.insert_one(info_dict)
                    count += 1
                else:

                    res_dict = dict(zip(res.Fields, res.Data))
                    df = pd.DataFrame.from_dict(res_dict)
                    df['wind_code'] = wind_code
                    df['commodity'] = cmd
                    df['type'] = 'long'
                    df2dict = df.to_dict(orient='index')

                    for di in df2dict:
                        dtemp = df2dict[di].copy()
                        dtemp['update_time'] = datetime.now()
                        dtemp.update(kwargs)
                        coll.insert_one(dtemp)
                    if last_trade_date < datetime.today():
                        info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                     'last_trade_date': last_trade_date, 'collection': collection,
                                     'status': 'Finished', 'update_time': datetime.now()}
                        coll_finished.insert_one(info_dict)
                        count += 1
                    else:
                        info_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                     'last_trade_date': last_trade_date, 'collection': collection,
                                     'status': 'Unfinished', 'update_time': datetime.now()}
                        coll_finished.insert_one(info_dict)
                        count += 1

            else:
                count += 1



        queryArgs_2 = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}, 'collection': collection,
                       'status': 'Unfinished'}
        projectionField_2 = ['wind_code', 'contract_issue_date', 'last_trade_date']
        unfinished_list = coll_finished.find(queryArgs_2, projectionField_2)

        print(unfinished_list)

        total = len(unfinished_list)
        count = 1



        for r in unfinished_list:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()
            wind_code = r['wind_code']
            issue_date = r['contract_issue_date']
            last_trade_date = r['last_trade_date']

            queryArgs = {'wind_code': wind_code}
            projectionField = ['wind_code', 'date']
            dt_end_res = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            dt_start_res = coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING).limit(1)
            dt_end_res = list(dt_end_res)
            dt_start_res = list(dt_start_res)


            # 如果数据库里没有数据
            if not dt_end_res and not dt_start_res:
                dt_end = min(last_trade_date, datetime.today())
                res = w.wset(tablename='openinterestranking', startdate=issue_date.strftime('%Y-%m-%d'),
                             enddate=dt_end.strftime('%Y-%m-%d'), varity=cmd, wind_code=wind_code,
                             order_by='long', ranks='all',
                             field='date,ranks,member_name,long_position,long_position_increase')


                if res.ErrorCode == -40522017:
                    raise Exception(u'数据提取量超限')

                if not res.Data and last_trade_date < datetime.today():
                    filter_con = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                  'last_trade_date': last_trade_date, 'collection': collection,
                                  'status': 'Unfinished'}
                    empty_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                  'last_trade_date': last_trade_date, 'collection': collection,
                                  'status': 'Null', 'update_time': datetime.now()}
                    coll_finished.update_one(filter_con, empty_dict)
                    count += 1

                elif not res.Data and last_trade_date >= datetime.today():
                    count += 1

                else:
                    res_dict = dict(zip(res.Fields, res.Data))
                    df = pd.DataFrame.from_dict(res_dict)
                    df['wind_code'] = wind_code
                    df['commodity'] = cmd
                    df['type'] = 'long'
                    df2dict = df.to_dict(orient='index')
                    for di in df2dict:
                        dtemp = df2dict[di].copy()
                        dtemp['update_time'] = datetime.now()
                        dtemp.update(kwargs)
                        coll.insert_one(dtemp)
                    if last_trade_date < datetime.today():
                        filter_con = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                      'last_trade_date': last_trade_date, 'collection': collection,
                                      'status': 'Unfinished'}
                        empty_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                      'last_trade_date': last_trade_date, 'collection': collection,
                                      'status': 'Null', 'update_time': datetime.now()}
                        coll_finished.update_one(filter_con, empty_dict)
                    count += 1


            elif dt_end_res[0]['date'] < last_trade_date:
                dt_start = dt_end_res[0]['date'] + timedelta(1)
                dt_end = min(last_trade_date, datetime.today())
                res = w.wset(tablename='openinterestranking', startdate=dt_start.strftime('%Y-%m-%d'),
                             enddate=dt_end.strftime('%Y-%m-%d'), varity=cmd,
                             wind_code=wind_code, order_by='long', ranks='all',
                             field='date,ranks,member_name,long_position,long_position_increase')

                if res.ErrorCode == -40522017:
                    raise Exception(u'数据提取量超限')

                if not res.Data:
                    print(wind_code, dt_start)
                    raise Exception('请检查%s为何没有新的持仓数据' % wind_code)

                res_dict = dict(zip(res.Fields, res.Data))
                df = pd.DataFrame.from_dict(res_dict)
                df['wind_code'] = wind_code
                df['commodity'] = cmd
                df['type'] = 'long'
                df2dict = df.to_dict(orient='index')
                for di in df2dict:
                    dtemp = df2dict[di].copy()
                    dtemp['update_time'] = datetime.now()
                    dtemp.update(kwargs)
                    coll.insert_one(dtemp)
                if last_trade_date < datetime.today():
                    filter_con = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                  'last_trade_date': last_trade_date, 'collection': collection,
                                  'status': 'Unfinished'}
                    empty_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                                  'last_trade_date': last_trade_date, 'collection': collection,
                                  'status': 'Finished', 'update_time': datetime.now()}
                    coll_finished.update_one(filter_con, empty_dict)
                count += 1


            elif dt_end_res[0]['date'] == last_trade_date:
                filter_con = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                              'last_trade_date': last_trade_date, 'collection': collection,
                              'status': 'Unfinished'}
                empty_dict = {'wind_code': wind_code, 'contract_issue_date': issue_date,
                              'last_trade_date': last_trade_date, 'collection': collection,
                              'status': 'Finished', 'update_time': datetime.now()}
                coll_finished.update_one(filter_con, empty_dict)
                count += 1

            else:
                print(dt_end_res)
                print(last_trade_date)
                raise Exception('请检查出现了另外的情况')

        sys.stdout.write('\n')
        sys.stdout.flush()


    def getFuturesInfoFromWind(self, collection, cmd, **kwargs):
        # 主要用于抓取wind里各合约的信息
        self.windConn()
        self.mongoConn()
        coll = self.db[collection]
        ptn_1 = re.compile('\w+(?=\.)')
        res_1 = ptn_1.search(cmd).group()
        ptn_2 = re.compile('(?<=\.)\w+')
        res_2 = ptn_2.search(cmd).group()

        # 国内合约名称与国外合约名称的规则不同
        # 如果品种属于中国的期货品种
        if res_2 in ['SHF', 'CZC', 'DCE', 'CFE', 'INE']:
            queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (res_1, res_2)}}
        # 如果品种是COMEX、NYMEX、ICE的合约，通常是品种+月份字母+年份数字+E(表示电子盘)+.交易所代码
        elif res_2 in ['CMX', 'NYM', 'IPE']:
            queryArgs = {'wind_code': {'$regex': '\A%s[FGHJKMNOUVXZ]\d+E\.%s\Z' % (res_1, res_2)}}
        # 如果品种是LME的合约
        elif res_2 in ['LME']:
            return
        else:
            print(cmd)
            raise Exception('请检查合约的名称，出现了新的交易所合约')

        dt_res = list(coll.find(queryArgs, ['contract_issue_date']).sort('contract_issue_date', pymongo.DESCENDING).limit(1))
        if dt_res:
            dt_last = dt_res[0]['contract_issue_date']
            dt_start = dt_last - timedelta(1)
        else:
            dt_start = datetime(1990, 1, 1)
        wres = w.wset(tablename='futurecc', startdate=dt_start.strftime('%Y-%m-%d'),
                      enddate=datetime.today().strftime('%Y-%m-%d'), wind_code=cmd)
        wfields = wres.Fields
        unit_total = len(wfields) * len(wres.Data[0])
        self.logger.info(u'共抓取了关于%s品种%d个单元格数据' % (cmd, unit_total))
        res = dict(zip(wfields, wres.Data))
        res.pop('change_limit')
        res.pop('target_margin')
        df = pd.DataFrame.from_dict(res)
        fu_info = df.to_dict(orient='index')
        for i, v in fu_info.items():
            v.update(kwargs)
            # 用来解决如果出现NaT的数据，无法传入数据库的问题
            if pd.isnull(v['last_delivery_month']):
                v['last_delivery_month'] = None
            if not coll.find_one({'wind_code': v['wind_code']}):
                v['update_time'] = datetime.now()
                coll.insert_one(v)
            elif coll.find_one({'wind_code': v['wind_code']})['last_trade_date'] != v['last_trade_date']:
                # 有些品种的wind_code会变，比如TA005.CZC之前是1005的合约，现在变成了2005的合约，真特么SB
                v['update_time'] = datetime.now()
                coll.update({'wind_code': v['wind_code']}, v)
        return

    def getFuturePriceFromWind(self, collection, contract, alldaytrade, update=1, **kwargs):
        self.windConn()
        self.mongoConn()
        coll = self.db['Information']
        finished_coll = self.db['FinishedContracts']
        queryArgs = {'wind_code': contract}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = coll.find_one(queryArgs, projectionField)

        if not searchRes:
            # WIND主力合约, 通常结构为商品代码.交易所代码的形式
            if update == 0:
                # 一次性抓取全样本数据
                # 起始日期为该商品的所有合约中最早的contract_issue_date
                ptn1 = re.compile('[A-Z]+(?=\.)')
                cmd1 = ptn1.search(contract).group()
                ptn2 = re.compile('(?<=\.)[A-Z]+')
                cmd2 = ptn2.search(contract).group()
                queryArgs = {'wind_code': {'$regex': '\A%s.+%s\Z' % (cmd1, cmd2)}}
                projectionField = ['wind_code', 'contract_issue_date']
                searchRes = coll.find(queryArgs, projectionField).sort('contract_issue_date',
                                                                       pymongo.ASCENDING).limit(1)
                start_date = list(searchRes)[0]['contract_issue_date']
                coll = self.db[collection]
            elif update == 1:
                coll = self.db[collection]
                queryArgs = {'wind_code': contract}
                projectionField = ['wind_code', 'date']
                searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
                start_date = list(searchRes)[0]['date'] + timedelta(1)

            if datetime.now().hour < 16 or alldaytrade:
                end_date = datetime.today() - timedelta(1)
            else:
                end_date = datetime.today()

        else:
            coll = self.db[collection]

            if update == 0:
                # 一次性全样本抓取数据
                # 起始日期为合约开始的日期
                # 当前时间还未收盘或者是全天交易的品种，则结束日期为前一天或合约最后交易日，否则为当天或合约最后交易日
                start_date = searchRes['contract_issue_date']
                if datetime.now().hour < 16 or alldaytrade:
                    end_date = min(datetime.today() - timedelta(1), searchRes['last_trade_date'])
                    if datetime.today() - timedelta(1) > searchRes['last_trade_date']:
                        finished_dict = searchRes.copy()
                else:
                    end_date = min(datetime.today(), searchRes['last_trade_date'])
                    if datetime.today() > searchRes['last_trade_date']:
                        finished_dict = searchRes.copy()

            elif update == 1:
                # 更新新的数据
                # 在数据库中查找到已有的数据的最后日期，然后+1作为起始日期
                # 结束日期同上
                queryArgs = {'wind_code': contract}
                projectionField = ['wind_code', 'date']
                mres = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
                dt_l = list(mres)[0]['date']
                if dt_l >= searchRes['last_trade_date']:
                    finished_dict = searchRes.copy()
                    finished_dict.update({'collection': collection, 'update_time': datetime.now()})
                    finished_coll.insert_one(finished_dict)
                    self.logger.info('%s合约进入到FinishedContract数据库' % contract)
                    return
                elif dt_l < searchRes['last_trade_date']:
                    # 对于合约变化的SB品种，如"TA005.CZC"，需要选择更近的时间
                    start_date = max(dt_l + timedelta(1), searchRes['contract_issue_date'])
                    if datetime.now().hour < 16 or alldaytrade:
                        end_date = min(datetime.today() - timedelta(1), searchRes['last_trade_date'])
                        # 这里需要注意为啥要增加一个比较，可参考CU0202合约，当时交易到2月8日，但是最后交易日是2月19日，由于春节放假导致的。
                    else:
                        end_date = min(datetime.today(), searchRes['last_trade_date'])


        if start_date > end_date:
            return

        res = w.wsd(contract, 'open, high, low, close, volume, amt, dealnum, oi, settle',
                    beginTime=start_date, endTime=end_date)

        if res.ErrorCode == -40520007:
            self.logger.info(u'WIND提取%s到%s的%s数据出现了错误' % (start_date, end_date, contract))
            return
        elif res.ErrorCode != 0:
            print(res)
            raise Exception(u'WIND提取数据出现了错误')
        else:
            unit_total = len(res.Data[0]) * len(res.Fields)
            self.logger.info(u'抓取%s合约%s到%s的市场价格数据，共计%d个' % (contract, start_date, end_date, unit_total))
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df['wind_code'] = contract
            price_dict = df.to_dict(orient='index')
            total = len(price_dict)
            count = 1
            print(u'抓取%s合约的数据' % contract)
            for di in price_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = price_dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)

                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

            if 'finished_dict' in locals():
                finished_dict.update({'collection': collection, 'update_time': datetime.now()})
                finished_coll.insert_one(finished_dict)
                self.logger.info('%s合约进入到FinishedContract数据库' % contract)

    def getFutureGroupPriceFromWind(self, collection, cmd, **kwargs):

        self.mongoConn()
        # 为了提高从wind抓取数据的效率，将已经抓取好的合约名存到数据库中。这样就不用总是去检查是不是有新数据需要抓取。

        coll = self.db['Information']
        finished_coll = self.db['FinishedContracts']

        # 在Information表中查找与该商品有关的所有合约
        # 只针对国内的商品合约
        # 查找之后还需要填加主力合约的代码

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        # 如果品种属于中国的期货品种
        if cmd2 in ['SHF', 'CZC', 'DCE', 'CFE', 'INE']:
            queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}}
        # 如果品种是COMEX、NYMEX、ICE的合约，通常是品种+月份字母+年份数字+E(表示电子盘)+.交易所代码
        # elif cmd2 in ['CMX', 'NYM', 'IPE']:
        #     queryArgs = {'wind_code': {'$regex': '\A%s[FGHJKMNQUVXZ]\d+E\.%s\Z' % (cmd1, cmd2)}}
        # 如果品种是LME的合约
        # elif cmd2 in ['LME']:
        elif cmd2 in ['LME', 'CMX', 'NYM', 'IPE']:
            queryArgs = {'wind_code': cmd}
        else:
            print(cmd)
            raise Exception('请检查合约的名称，出现了新的交易所合约')

        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = coll.find(queryArgs, projectionField)


        queryArgs_1 = queryArgs.copy()
        queryArgs_1.update({'collection': collection})
        projectionField_1 = ['wind_code', 'contract_issue_date', 'last_trade_date']
        finishedRes = finished_coll.find(queryArgs_1, projectionField_1)

        contract_list = [(s['wind_code'], s['contract_issue_date'], s['last_trade_date']) for s in searchRes]
        finished_list = [(s['wind_code'], s['contract_issue_date'], s['last_trade_date']) for s in finishedRes]
        unfinished_list = list(set(contract_list).difference(set(finished_list)))
        unfinished_list = [s[0] for s in unfinished_list]

        # 增加主力合约代码
        unfinished_list.append(cmd)
        print(unfinished_list)
        coll = self.db[collection]
        for d in unfinished_list:
            if coll.find_one({'wind_code': d}):
                self.getFuturePriceFromWind(collection=collection, contract=d, update=1, **kwargs)
            else:
                self.getFuturePriceFromWind(collection=collection, contract=d, update=0, **kwargs)

    def getWSDFromWind(self, collection, cmd, fields, tradingcalendar, alldaytrade, **kwargs):
        self.windConn()
        self.mongoConn()
        coll = self.db[collection]
        if coll.find_one({'wind_code': cmd}):
            queryArgs = {'wind_code': cmd}
            projectionField = ['wind_code', 'date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
        else:
            start_date = datetime.strptime('19900101', '%Y%m%d')

        if alldaytrade:
            end_date = datetime.today() - timedelta(1)
        else:
            end_date = datetime.today()

        if start_date > end_date:
            return

        if tradingcalendar == 'SHSE':
            tradingcalendar = ''

        res = w.wsd(cmd, fields=fields, beginTime=start_date, endTime=end_date, TradingCalendar=tradingcalendar)
        if res.ErrorCode == -40520007 and res.Fields == ['OUTMESSAGE']:
            print(res)
            print('%s没有新的数据' % cmd)
            return
        elif res.ErrorCode != 0:
            print(res)
            raise Exception(u'WIND使用wsd提取数据出现了错误')
        else:
            unit_total = len(res.Data[0]) * len(res.Fields)
            self.logger.info(u'使用WSD抓取%s数据%s到%s的数据，共计%d个' % (cmd, start_date, end_date, unit_total))
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df.dropna(axis=0, how='all', subset=fields, inplace=True)
            df['wind_code'] = cmd
            df2dict = df.to_dict(orient='index')

            total = len(df2dict)
            count = 1
            for di in df2dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = df2dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)
                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

    def getEDBFromWind(self, collection, edb_code, **kwargs):
        self.windConn()
        self.mongoConn()
        coll = self.db[collection]
        if coll.find_one({'wind_code': edb_code}):
            queryArgs = {'wind_code': edb_code}
            projectionField = ['wind_code', 'date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
            end_date = datetime.today()
        else:
            start_date = datetime.strptime('19900101', '%Y%m%d')
            end_date = datetime.today()

        if start_date > end_date:
            return
        res = w.edb(edb_code, start_date, end_date, 'Fill=previous')

        if res.ErrorCode == 0:
            unit_total = len(res.Data[0]) * len(res.Fields)
            self.logger.info(u'抓取EDB%s数据%s到%s的数据，共计%d个' % (edb_code, start_date, end_date, unit_total))
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df['wind_code'] = edb_code
            df2dict = df.to_dict(orient='index')

            total = len(df2dict)
            count = 1
            print('抓取%s数据' % edb_code)
            for di in df2dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                # 该判断是必要的，因为如果日期是之后的，而数据没有，edb方法会返回最后一个数据
                if coll.find_one({'wind_code': edb_code, 'date': datetime.strptime(str(di), '%Y-%m-%d')}):
                    self.logger.info(u'%s该数据已经存在于数据库中，没有抓取' % edb_code)
                    continue

                dtemp = df2dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)
                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

        elif res.ErrorCode == -40521008:
            print(edb_code)
            print(res)
            self.logger.info('%s该数据没有最新数据' % edb_code)
        else:
            print(edb_code)
            print(res)
            raise Exception(u'WIND提取数据出现了错误')


    def getPriceFromRT(self, collection, cmd, data_type=None, interval='daily', save_mode='MongoDB', save_path=None,
                       category=None, **kwargs):
        """
        futures是来判断是否抓取期货数据，涉及到字段问题
        这里的一个非常重要的问题就是交易时间
        比如现在北京时间凌晨1点，欧美交易所的时间仍是昨天，此时如果抓取数据，虽然是抓昨天的数据，但是交易依然在进行，所以此时会出错
        """
        if not ek.get_app_key():
            self.rtConn()

        print(cmd)
        # 路透每回提取的数据最大只有50000条，如果要提取更多的数据，需要调整日期
        max_items = 50000
        if interval == 'daily':
            max_periods = max_items
            log_flag = '日度'
            para_dict = {'days': 1}

        elif interval == 'minute':
            max_periods = int(max_items / 60 / 24) - 5
            log_flag = '分钟'
            para_dict = {'minutes': 1}



        if save_mode in ['MongoDB', 'MongoDB CSV']:

            self.mongoConn()

            coll = self.db[collection]

            if coll.find_one({'tr_code': cmd}):
                queryArgs = {'tr_code': cmd}
                queryArgs.update(kwargs)
                projectionField = ['tr_code', 'date']
                searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
                start_date = list(searchRes)[0]['date'] + timedelta(**para_dict)
                end_date = datetime.today() - timedelta(1)
                end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                if interval == 'minute':
                    start_date = datetime.today() - timedelta(days=400)
                    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                elif interval == 'daily':
                    start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
                end_date = datetime.today() - timedelta(1)
                end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

            if start_date > end_date:
                return

            if data_type == 'futures':
                fields_dict = {'fields': ['HIGH', 'LOW', 'OPEN', 'CLOSE', 'VOLUME']}
            elif data_type == 'swap' or data_type == 'spot':
                fields_dict = {'fields': ['CLOSE']}
            elif data_type == 'foreign exchange':
                fields_dict = {'fields': ['HIGH', 'LOW', 'OPEN', 'CLOSE', 'COUNT']}
            else:
                fields_dict = {}

            if interval == 'minute':
                fields_dict = {}


            start_temp = start_date
            end_temp = min(start_temp + timedelta(days=max_periods), end_date)

            df_total = pd.DataFrame()

            while True:
                try:
                    res = ek.get_timeseries(cmd, start_date=start_temp, end_date=end_temp, interval=interval,
                                            **fields_dict)
                    self.logger.info('从路透提取了%s合约%s到%s的%s数据' % (cmd, start_temp, end_temp, log_flag))
                    df_total = pd.concat((df_total, res))
                    start_temp = end_temp + timedelta(**para_dict)
                    end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                    if start_temp >= end_temp:
                        break

                except ek.eikonError.EikonError as e:
                    if 'No data available for the requested date range' in e.message:
                        start_temp = end_temp + timedelta(**para_dict)
                        end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                        if start_temp >= end_temp:
                            break
                        continue
                    elif 'Error code 401 | Client Error: Eikon API Proxy requires authentication' in e.message:
                        self.logger.info('路透终端没有登录')
                        return
                    elif 'Error code 401 | Eikon Proxy not installed or not running' in e.message:
                        self.logger.info('路透终端没有登录')
                        return
                    else:
                        raise Exception(e)

            unit_total = len(df_total.values.flatten())
            self.logger.info(u'抓取%s%s到%s的数据，共计%d个' % (cmd, start_date, end_date, unit_total))

            df_total['tr_code'] = cmd
            total_dict = df_total.to_dict(orient='index')

            total = len(total_dict)
            count = 1
            print(u'抓取路透%s合约的数据' % cmd)
            for di in total_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = total_dict[di].copy()
                dtemp['date'] = di
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)

                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

        elif save_mode in ['CSV', 'MongoDB CSV']:
            if not save_path:
                print(cmd)
                raise Exception('没有提供保存的CSV路径save_path字段')
            if not category:
                print(cmd)
                raise Exception('没有提供该合约的category字段')
            path_new = os.path.join(save_path, collection, category)
            if not os.path.exists(path_new):
                os.makedirs(path_new)
            file_path = os.path.join(path_new, '%s.csv' % cmd)

            if not os.path.exists(file_path):
                if interval == 'minute':
                    start_date = datetime.today() - timedelta(days=400)
                    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                elif interval == 'daily':
                    start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
                end_date = datetime.today() - timedelta(days=1)
                end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

                start_temp = start_date
                end_temp = min(start_temp + timedelta(days=max_periods), end_date)

                df_total = pd.DataFrame()

                while True:
                    try:
                        res = ek.get_timeseries(cmd, start_date=start_temp, end_date=end_temp, interval=interval)
                        self.logger.info('从路透提取了%s合约%s到%s的%s数据' % (cmd, start_temp, end_temp, log_flag))
                        df_total = pd.concat((df_total, res))
                        start_temp = end_temp + timedelta(**para_dict)
                        end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                        if start_temp >= end_temp:
                            break

                    except ek.eikonError.EikonError as e:
                        if 'No data available for the requested date range' in e.message:
                            start_temp = end_temp + timedelta(**para_dict)
                            end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                            if start_temp >= end_temp:
                                break
                            continue
                        elif 'Error code 401 | Client Error: Eikon API Proxy requires authentication' in e.message:
                            self.logger.info('路透终端没有登录')
                            return
                        elif 'Invalid RIC' in e.message:
                            self.logger.info('%s代码是无效代码' % cmd)
                            return
                        else:
                            raise Exception(e)
                    except ValueError as e:
                        if 'Empty data passed with indices specified' in e.args[0]:
                            self.logger.info('在%s到%s时间段内%s没有数据' % (start_temp, end_temp, cmd))
                            start_temp = end_temp + timedelta(**para_dict)
                            end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                            if start_temp >= end_temp:
                                break
                            continue
                        else:
                            raise Exception(e)

                if not df_total.empty:
                    df_total.to_csv(file_path)
                    self.logger.info('生成了%s合约的%s数据CSV文件' % (cmd, log_flag))



                # try:
                #     res = ek.get_timeseries(cmd, start_date=start_date, end_date=end_date, interval=interval)
                #     print(res)
                # except ek.eikonError.EikonError as e:
                #     print('更新路透%s数据出现错误' % cmd)
                #     print(e)
                #     return

            else:
                # 如果文件小于100M，直接读取
                if (os.path.getsize(file_path) / 1024 / 1024) < 100.:
                    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    columns_list = df.columns
                    exists_dt = df.index[-1]
                    exists_dt = exists_dt.to_pydatetime()

                    start_date = exists_dt + timedelta(**para_dict)
                    end_date = datetime.today() - timedelta(days=1)
                    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

                    start_temp = start_date
                    end_temp = min(start_temp + timedelta(days=max_periods), end_date)

                    if start_temp > end_temp:
                        return

                    df_total = pd.DataFrame()

                    while True:
                        try:
                            res = ek.get_timeseries(cmd, start_date=start_temp, end_date=end_temp, interval=interval)
                            res = res[columns_list]
                            self.logger.info('从路透提取了%s合约%s到%s的%s数据' % (cmd, start_temp, end_temp, log_flag))
                            df_total = pd.concat((df_total, res))
                            start_temp = end_temp + timedelta(**para_dict)
                            end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                            if start_temp >= end_temp:
                                break

                        except ek.eikonError.EikonError as e:
                            if 'No data available for the requested date range' in e.message:
                                start_temp = end_temp + timedelta(**para_dict)
                                end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                                if start_temp >= end_temp:
                                    break
                                continue
                            elif 'Invalid RIC' in e.message:
                                self.logger.info('%s代码已经无效，该合约可能已经到期，请检查' % cmd)
                                break

                            elif 'Error code 401 | Client Error: Eikon API Proxy requires authentication' in e.message:
                                self.logger.info('路透终端没有登录')
                                return

                            else:
                                raise Exception(e)
                        except ValueError as e:
                            if 'Empty data passed with indices specified' in e.args[0]:
                                self.logger.info('在%s到%s时间段内%s没有数据' % (start_temp, end_temp, cmd))
                                start_temp = end_temp + timedelta(**para_dict)
                                end_temp = min(start_temp + timedelta(days=max_periods), end_date)
                                if start_temp >= end_temp:
                                    break
                                continue
                            else:
                                raise Exception(e)


                    if not df_total.empty:
                        df_total.to_csv(file_path, mode='a', header=False)
                        self.logger.info('修改了%s合约的%s数据CSV文件' % (cmd, log_flag))

        return

    def getDataFromCSV(self, collection, cmd, path, field, **kwargs):
        """
        从csv文件中导入数据到数据库
        """
        self.mongoConn()
        coll = self.db[collection]
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        print(cmd)
        df = df[[cmd]]
        df.dropna(inplace=True)
        df = df.astype('float64')  # 将数据转成浮点型，否则存入数据库中会以NumberLong的数据类型

        # 可以进行更新
        # if coll.find_one({'commodity': cmd}):
        #     searchRes = coll.find({'commodity': cmd}, ['date']).sort('date', pymongo.DESCENDING).limit(1)
        #     start_date = list(searchRes)[0]['date']
        #     df = df[df.index > start_date]
        # else:
        #     start_date = df.index[0]
        #
        # if df.empty:
        #     return

        start_date = df.index[0]

        # unit_total = len(df.values.flatten())
        # self.logger.info('抓取%s%s之后的数据，共计%d个' % (cmd, start_date, unit_total))

        # 关于编码的问题，如果是中文，需要将unicode转成str
        df.rename(columns={cmd: field}, inplace=True)

        df['commodity'] = cmd
        for k, v in kwargs.items():
            df[k] = v

        queryArgs = {'commodity': cmd, 'date': {'$gte': start_date}}
        queryArgs.update(kwargs)
        projectionFields = ['date', field]
        exist_res = coll.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
        exist_res = list(exist_res)
        if exist_res:
            exist_df = pd.DataFrame.from_records(exist_res, index='date')
            exist_df.drop(columns='_id', inplace=True)

            sub_res = exist_df - df[[field]]
            isEqual = sub_res == 0
            diff_res = sub_res.loc[~isEqual.values.flatten()]

            count = 0
            for i in diff_res.index:
                if i not in df.index:
                    # 该数值不在csv中
                    continue
                elif i in exist_df.index:
                    # 该数值在csv和数据库中都存在，但是不一样
                    self.logger.info('%s在%s这一天的%s数据与数据库中已经存在的数据不一致' % (cmd, i, field))
                    self.logger.info('数据库存在的数据：%s, csv存在的数据：%s' % (exist_df.loc[i, field], df.loc[i, field]))
                    queryArgs = {'commodity': cmd, 'date': i}
                    queryArgs.update(kwargs)
                    coll.delete_many(queryArgs)
                    res_dict = df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    coll.insert_one(res_dict[i])
                    count += 1
                else:
                    # 该数值是新增的数据库中没有的数据
                    self.logger.info('%s在%s这一天的%s数据进入数据库' % (cmd, i, field))
                    res_dict = df.loc[[i]].to_dict(orient='index')
                    res_dict[i]['date'] = i
                    res_dict[i]['update_time'] = datetime.now()
                    coll.insert_one(res_dict[i])
                    count += 1
            self.logger.info('%s更新了%d条%s数据' % (cmd, count, field))
        else:
            count = 0
            for i in df.index:
                self.logger.info('%s在%s这一天的%s数据进入数据库' % (cmd, i, field))
                res_dict = df.loc[[i]].to_dict(orient='index')
                res_dict[i]['date'] = i
                res_dict[i]['update_time'] = datetime.now()
                coll.insert_one(res_dict[i])
                count += 1
            self.logger.info('%s更新了%d条%s数据' % (cmd, count, field))




        # res_dict = df.to_dict(orient='index')
        # total = len(res_dict)
        # count = 1
        # print('抓取%s数据' % cmd)
        # for di in res_dict:
        #     process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
        #     sys.stdout.write('\r' + process_str + '【已完成%5.2f%%】' % (count * 100. / total))
        #     sys.stdout.flush()
        #
        #     exist_res = coll.find({'commodity': cmd, 'date': di}, ['date', field])
        #     exist_res = list(exist_res)
        #     if not exist_res:
        #         dtemp = res_dict[di].copy()
        #         dtemp['date'] = di
        #         dtemp['update_time'] = datetime.now()
        #         coll.insert_one(dtemp)
        #     elif field not in exist_res[0] or exist_res[0][field] != res_dict[di][field]:
        #         coll.delete_many({'commodity': cmd, 'date': di})
        #         dtemp = res_dict[di].copy()
        #         dtemp['date'] = di
        #         dtemp['update_time'] = datetime.now()
        #         coll.insert_one(dtemp)
        #     else:
        #         count += 1
        #         continue
        #
        #
        #
        #     count += 1
        #
        # sys.stdout.write('\n')
        # sys.stdout.flush()

    def getDateSeries(self, collection, cmd, **kwargs):
        """从WIND导入交易日期时间序列"""
        self.mongoConn()
        self.windConn()
        coll = self.db[collection]
        # 如果已经有了日期数据，这里是需要重新抓当年年份和下一年年份的数据
        # 当前年份是针对12月最后一天是否为假期，因为这一天是由次年元旦来决定的

        if coll.find_one({'exchange': cmd}):
            queryArgs = {'exchange': cmd}
            projectionField = ['date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            exist_date = list(searchRes)[0]['date']
            current_year = datetime.today().year
            current_month = datetime.today().month

            # 为了防止出现在不同时间点上无法更新当年最后一天是不是交易日
            start_date = datetime(min(current_year, exist_date.year), 1, 1)

            if current_month > 11:
                end_date = datetime(current_year + 1, 12, 31)
            else:
                end_date = datetime(current_year, 12, 31)

            queryArgs = {'exchange': cmd, 'date': {'$gte': start_date, '$lte': end_date}}
            projectionField = ['date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            df_exist = pd.DataFrame.from_records(searchRes)
            df_exist.index = df_exist['date']
            df_exist.drop(columns='_id', inplace=True)

            if cmd == 'SHSE':
                res = w.tdays(beginTime=start_date, endTime=end_date)

            else:
                res = w.tdays(beginTime=start_date, endTime=end_date, TradingCalendar=cmd)

            df_res = pd.DataFrame({'date': res.Data[0]}, index=res.Times)
            df_total = df_res.join(df_exist, how='outer', lsuffix='New', rsuffix='Old')
            df_total['isEqual'] = df_total['dateNew'] == df_total['dateOld']

            # 出现不同的交易日期
            df_diff = df_total[~df_total['isEqual']]

            if df_diff.empty:
                return
            else:
                # 对于dateOld中要删除，dateNew中的要增加
                for i in df_diff['dateOld']:
                    if pd.isna(i):
                        continue
                    queryArgs = {'exchange': cmd, 'date': i}
                    self.logger.info('删除了%s的错误的交易日期%s' % (cmd, i))
                    delete_record = coll.delete_many(queryArgs)
                    if delete_record.deleted_count != 1:
                        raise Exception('删除了%s多个日期%s' % (cmd, i))
                for i in df_diff['dateNew']:
                    if pd.isna(i):
                        continue
                    res_dict = {'date': i, 'exchange': cmd, 'update_time': datetime.now()}
                    res_dict.update(kwargs)
                    self.logger.info('增加了%s的交易日期%s' % (cmd, i))
                    coll.insert_one(res_dict)

        else:
            start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
            current_year = datetime.today().year
            current_month = datetime.today().month
            if current_month > 11:
                end_date = datetime(current_year + 1, 12, 31)
            else:
                end_date = datetime(current_year, 12, 31)

            if cmd == 'SHSE':
                res = w.tdays(beginTime=start_date, endTime=end_date)
            else:
                res = w.tdays(beginTime=start_date, endTime=end_date, TradingCalendar=cmd)

            total = len(res.Data[0])
            # count = 1

            print(u'更新交易日期数据')

            for r in res.Data[0]:
                # process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                # sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                # sys.stdout.flush()
                res_dict = {'date': r, 'exchange': cmd, 'update_time': datetime.now()}
                res_dict.update(kwargs)
                self.logger.info('插入了%s的交易日期%s' % (cmd, r))
                coll.insert_one(res_dict)
                # count += 1
            self.logger.info(u'共更新了%s个交易日期数据进入到数据库' % total)
            # sys.stdout.write('\n')
            # sys.stdout.flush()

    def combineMainContract(self, collection, cmd, method, month_list=range(1, 13)):
        self.mongoConn()
        source = self.db['FuturesMD']  # 期货市场价格表
        target = self.db[collection]
        info_source = self.db['Information']  # 期货合约信息表
        dt_source = self.db['DateDB']  # 金融市场交易日期表，使用的SHSE，好像与SHF会有不同。。。。MB，不想改了。。有需要再说

        #  找到金融市场交易日期序列
        projectionFields = ['date']
        res = dt_source.find(projection=projectionFields).sort('date', pymongo.ASCENDING)
        dt_list = []
        for r in res:
            dt_list.append(r['date'])
        dt_list = np.array(dt_list)

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        #  df_res是符合要求的合约
        if method == 'LastMonthEnd':
            month_list = ['%02d' % i for i in month_list]
            month_re = '|'.join(month_list)
            queryArgs = {'wind_code': {'$regex': '\A%s(?=\d+).+(%s)(?=\.).+(?<=[\d+\.])%s\Z' % (cmd1, month_re, cmd2)}}
            projectionFields = ['wind_code', 'last_trade_date']
            res = info_source.find(queryArgs, projectionFields)
            res_copy = []
            for r in res:
                yr = r['last_trade_date'].year
                mon = r['last_trade_date'].month
                r['switch_date'] = dt_list[dt_list < datetime(yr, mon, 1)][-1]
                res_copy.append(r)
            df_res = pd.DataFrame.from_records(res_copy)
            df_res.drop(columns=['_id'], inplace=True)
        elif method == 'OI':
            queryArgs = {'wind_code': {'$regex': '\A%s(?=\d+).+(?<=[\d+\.])%s\Z' % (cmd1, cmd2)}}
            projectionFields = ['wind_code', 'date', 'OI']
            res = source.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns=['_id'], inplace=True)
            df_rolling_oi = pd.DataFrame()
            for v in df_res['wind_code'].unique():
                df_v = df_res[df_res['wind_code'] == v]
                temp = df_v['OI'].rolling(window=10).mean()
                df_v['OI_10_mean'] = temp
                df_rolling_oi = pd.concat([df_rolling_oi, df_v], ignore_index=True)


            # df_group1 = df_res.groupby('wind_code')
            # df_res1 = df_group1.apply(lambda x: x['OI'].rolling(window=10).mean())
            # df_res1.reset_index(inplace=True)
            # print df_res1

            df_group = df_rolling_oi.groupby('date')
            df_res = df_group.apply(lambda x: x[x['OI_10_mean'] == x['OI_10_mean'].max()])
            df_res.reset_index(drop=True, inplace=True)


            print(df_res)
            # for index, data in df_group:
            #     print index
            #     print data



        queryArgs = {'name': '%s_MC_%s' % (cmd, method)}
        projectionFields = ['date', 'name']
        searchRes = target.find_one(queryArgs, projectionFields)
        # 如果是第一次生成
        if not searchRes:
            # 针对已有的合约交易日期，去重之后，得到该品种的所有交易日期
            queryArgs = {'wind_code': {'$regex': '\A%s(?=\d+).+(?<=[\d+\.])%s\Z' % (cmd1, cmd2)}}
            projectionFields = ['date']
            res = source.find(queryArgs, projectionFields).sort('date', pymongo.ASCENDING)
            df_trade = pd.DataFrame.from_records(res)
            df_trade.drop(columns=['_id'], inplace=True)
            df_trade.drop_duplicates(inplace=True)
            df_trade.index = range(len(df_trade))
        else:
            res = target.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(1)
            dt_start = list(res)[0]['date']
            queryArgs = {'wind_code': {'$regex': '\A%s(?=\d+).+(?<=[\d+\.])%s\Z' % (cmd1, cmd2)}}
            projectionFields = ['date']
            res = source.find(queryArgs, projectionFields).sort('date', pymongo.DESCENDING).limit(1)
            dt_end = list(res)[0]['date']
            df_trade = pd.DataFrame({'date': dt_list[(dt_list > dt_start) * (dt_list <= dt_end)]})

        df1 = pd.merge(left=df_trade, right=df_res, left_on='date', right_on='switch_date', sort=True, how='outer')
        df1[['last_trade_date', 'switch_date', 'wind_code']] = df1[['last_trade_date', 'switch_date', 'wind_code']].\
            fillna(method='bfill')
        df1.dropna(inplace=True)

        df_final = pd.DataFrame()
        for c in df1['wind_code'].unique():
            queryArgs = {'wind_code': c}
            res = source.find(queryArgs).sort('date', pymongo.ASCENDING)
            df_c = pd.DataFrame.from_records(res)
            df_c.dropna(axis=1, how='all', inplace=True)
            df_c.drop(columns=['_id', 'wind_code', 'update_time'], inplace=True)
            df_n = pd.merge(left=df1[df1['wind_code'] == c], right=df_c, on='date', how='left', sort=True)
            df_final = pd.concat([df_final, df_n], ignore_index=True, sort=False)

        insert_dict = df_final.to_dict(orient='records')

        count = 1
        total = len(insert_dict)
        print(u'生成主力合约%s_MC_%s' % (cmd, method))
        for r in insert_dict:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()
            r['name'] = '%s_MC_%s' % (cmd, method)
            r['remain_days'] = float((r['last_trade_date'] - r['date']).days)
            r['update_time'] = datetime.now()
            target.insert_one(r)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()



if __name__ == '__main__':
    # DataSaving().getFuturesPriceAuto(security='M')
    # DataSaving().getFuturesInfoFromWind('BU.SHF')

    # DataSaving().getFuturePriceAutoFromWind('TA')

    # DataSaving().getMainFuturePriceAutoFromWind(cmd='B.IPE', alldaytrade=1)
    # DataSaving().getAllFuturesInfoFromWind()
    # DataSaving().test('J.DCE')
    # DataSaving().getFXFromWind('即期汇率:美元兑人民币')
    # DataSaving().getFuturePriceFromRT('LCO')
    a = DataSaving(host='localhost', port=27017, usr='yuanjie', pwd='yuanjie', db='CBNB',
                   log_path="E:\\CBNB\\BackTestSystem\\data_saving.log")
    # a.getFuturesInfoFromWind(collection='Information', cmd='BU.SHF')
    # a.getFuturePriceFromWind('FuturesMD', 'TA.CZC', alldaytrade=0)
    # a.getPriceFromRT('FuturesMD', cmd='LCOc1', type='futures')
    # a.getDataFromCSV(collection='SpotMD', cmd='PX', path='F:\\CBNB\\CBNB\\BackTestSystem\\lib\\data\\supplement_db\\PX.csv')
    # res = w.wset(tablename='futurecc', startdate='2018-01-01', enddate='2018-10-19', wind_code='TA.CZC')
    # print res

    # a.getDateSeries(collection='DateDB', cmd='SHSE', frequecy='Daily')
    # a.getFuturesOIRFromWind(collection='FuturesOIR', cmd='L.DCE')
    # a.getFuturesMinPriceFromWind(collection='FuturesMinMD', ctr='RB1905.SHF', frequency='10min')


