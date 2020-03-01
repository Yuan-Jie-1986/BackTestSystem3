"""
回测中所使用的数据类
auther: YUANJIE
"""

import numpy as np
import pandas as pd

# 需要导入的数据构造类

class DataClass(object):

    '''
    主要是用来存储数据信息的，通常是时间序列的数据
    可以是日频/周频/月频/分钟数据
    '''

    def __init__(self, nm, freq=''):
        self.nm = nm
        self.ts_data_field = []  # 用来存储时间序列数据的变量名
        self.ts_string_field = []  # 用来存储时间序列的字符的变量名
        self.frequency = freq  # 该数据的频率，可选范围：daily, weekly, monthly, minutes

    def add_ts(self, ts):
        # self.ts是该数据类里的时间序列
        # self.dt是转换成日期的时间序列
        self.ts = np.array(ts)
        self.dt = self.ts.copy()

    # 检查时间长度与时间序列的变量长度是否一致
    def check_len(self):
        dt_len = len(self.ts)
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
    def rearrange_ts_data(self, ts_new):
        con1 = np.in1d(self.ts, ts_new)
        con2 = np.in1d(ts_new, self.ts)
        for field in self.ts_data_field:
            temp = np.ones(len(ts_new)) * np.nan
            temp[con2] = getattr(self, field)[con1]
            setattr(self, field, temp)
        for field in self.ts_string_field:
            temp = np.array(len(ts_new) * [None])
            temp[con2] = getattr(self, field)[con1]
            setattr(self, field, temp)
        self.add_ts(ts_new)

    # 对于分钟数据，通过这个函数可以得到具体的日期序列
    def min_2_dt(self):
        if self.frequency == 'minutes':
            self.dt = [t.replace(hour=0, minute=0, second=0, microsecond=0) for t in self.ts]
            self.dt = set(self.dt)
            self.dt = np.array(list(self.dt))
            self.dt.sort()
        else:
            return

    # 对于周度和月度数据进行数据的整理，生成新的数据
    @auto_check
    def long_2_dt(self):
        if self.frequency in ['weekly', 'monthly']:
            dt = pd.date_range(start=self.ts[0], end=self.ts[-1])
            dt = np.array(dt.to_pydatetime())
            self.rearrange_ts_data(dt)
            self.frequency = 'daily'
            ts_field = self.ts_data_field + self.ts_string_field
            for field in ts_field:
                self.fillna_ts_data(field)
        else:
            return