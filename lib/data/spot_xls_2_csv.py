#coding=utf-8
import pandas as pd
import numpy as np
import os

## 现货价格的文件处理
path = 'E:\CBNB\BackTestSystem\lib\data\supplement_db'
file_nm = u'E:\CBNB\BackTestSystem\lib\data\supplement_db\spot_price.xlsx'


col_dict = {u'LL神华煤化工价格': 'LL',
            u'PP华东现货价': 'PP',
            u'甲醇华东（江苏地区）': 'MA',
            u'现货（常州sg-5低端价）': 'PVC',
            u'TA内盘人民币价': 'PTA',
            u'国产重交-山东': u'沥青',
            u'MEG': 'MEG'}

spot_xls = pd.read_excel(file_nm, index_col=u'日期')
spot_xls = spot_xls[col_dict.keys()]
for c in spot_xls.columns:
    spot_c = spot_xls[[c]]
    spot_c.replace(0, np.nan, inplace=True)
    spot_c.dropna(inplace=True)
    spot_c.to_csv(path + '\\' + col_dict[c] + '.csv', encoding='utf-8')


## 库存数据的文件处理
path = 'E:\CBNB\BackTestSystem\lib\data\inventory_db'
file_nm = u'E:\CBNB\BackTestSystem\lib\data\inventory_db\库存.xlsx'

col_dict = {'PTA': 'PTA',
            'MEG': 'MEG',
            'LL': 'LL',
            'PP': 'PP',
            u'螺纹': 'RB',
            u'热卷': 'HC',
            u'甲醇': 'MA',
            u'沥青': 'BU'}

inventory_xls = pd.read_excel(file_nm, index_col='date')
inventory_xls = inventory_xls[col_dict.keys()]
for c in inventory_xls:
    print c
    file_c = os.path.join(path, '%s.csv' % col_dict[c])
    if os.path.exists(file_c):
        df_c = pd.read_csv(file_c, index_col='date', parse_dates=True)
        df_c_new = inventory_xls[[c]]
        df_c_new.rename(columns={c: col_dict[c]}, inplace=True)
        # print df_c_new
        # index_1 = df_c.index
        # index_2 = df_c_new.index
        # index_intersect = set(index_1).intersection(set(index_2))
        # print index_1, index_2
        # print index_intersect
        # print col_dict[c], c
        df_c = pd.merge(df_c, df_c_new, left_index=True, right_index=True, on=col_dict[c], how='outer')
        df_c.to_csv(os.path.join(path, '%s.csv' % col_dict[c]))
    else:
        df_c = inventory_xls[[c]]
        df_c.rename(columns={c: col_dict[c]}, inplace=True)
        df_c.to_csv(os.path.join(path, '%s.csv' % col_dict[c]))






