
import pandas as pd
import numpy as np
import os

## 现货价格的文件处理
path = 'E:\CBNB\BackTestSystem3\lib\data\supplement_db'
file_nm = u'E:\CBNB\BackTestSystem3\lib\data\supplement_db\spot_price.xlsx'


col_dict = {'LL神华煤化工价格': 'LL_SHENHUA',
            'LL华东': 'LL_HUADONG',
            'PP华东现货价': 'PP',
            '甲醇华东（江苏地区）': 'MA',
            '现货（常州sg-5低端价）': 'PVC',
            'TA内盘人民币价': 'PTA',
            '国产重交-山东': u'沥青',
            'MEG': 'MEG',
            'PX': 'PX',
            '泰国STR20混合胶': 'RU',
            '华北融指1线性（天津9085）': 'RONGZHI_HUABEI',
            '华北重包': 'ZHONGBAO_HUABEI',
            '地膜': 'DIMO',
            '双防膜': 'SHUANGFANGMO',
            '缠绕膜': 'CHANRAOMO',
            'BOPP膜': 'BOPPMO',
            '华北电石法 SG5': 'SG5_HUABEI',
            '电石华北 山东': 'DIANSHI_SD',
            '液氯华北': 'YELV_HUABEI',
            '华北电价': 'DIANJIA_HUABEI',
            'POY150D/48F': 'POY',
            '杯胶（泰铢/kg)': 'BEIJIAO'}

spot_xls = pd.read_excel(file_nm, index_col='日期')
spot_xls = spot_xls[col_dict.keys()]
for c in spot_xls.columns:
    spot_c = spot_xls[[c]].copy()
    spot_c.replace(0, np.nan, inplace=True)
    spot_c.dropna(inplace=True)
    spot_c.to_csv(path + '\\' + col_dict[c] + '.csv', encoding='utf-8')

## 进出口数据的文件处理

path = 'E:\CBNB\BackTestSystem3\lib\data\imexport_db'
file_nm = 'E:\CBNB\BackTestSystem3\lib\data\imexport_db\jinkou.csv'

col_dict = {'bz cfr': 'bz_cfr',
            'bz rmb': 'bz_rmb',
            'sm cfr': 'sm_cfr',
            'sm rmb': 'sm_rmb',
            'meg cfr': 'meg_cfr',
            'meg rmb': 'meg_rmb',
            'ma cfr': 'ma_cfr',
            'ma rmb': 'ma_rmb',
            'lpg cfr': 'lpg_cfr',
            'lpg rmb': 'lpg_rmb',
            'pp cfr': 'pp_cfr',
            'pp rmb': 'pp_rmb',
            'll cfr': 'll_cfr',
            'll rmb': 'll_rmb',
            'ld cfr': 'ld_cfr',
            'ld rmb': 'ld_rmb',
            'hd cfr': 'hd_cfr',
            'hd rmb': 'hd_rmb',
            'px cfr': 'px_cfr',
            'px rmb': 'px_rmb',
            'PBF cfr': 'PBF_cfr',
            'PBF rmb': 'PBF_rmb',
            'NHGF cfr': 'NHGF_cfr',
            'NHGF rmb': 'NHGF_rmb',
            'JMBF cfr': 'JMBF_cfr',
            'JMBF rmb': 'JMBF_rmb',
            'MACF cfr': 'MACF_cfr',
            'MACF rmb': 'MACF_rmb',
            'YDF cfr': 'YDF_cfr',
            'YDF rmb': 'YDF_rmb',
            'zc rmb': 'zc_rmb',
            'zc cfr': 'zc_cfr',
            'bu rmb': 'bu_rmb',
            'bu cfr': 'bu_cfr',
            'ru rmb': 'ru_rmb',
            'ru cfr': 'ru_cfr'}
jinkou_df = pd.read_csv(file_nm, index_col='dt')
jinkou_df = jinkou_df[col_dict.keys()]
for c in jinkou_df.columns:
    jinkou_c = jinkou_df[[c]].copy()
    jinkou_c.replace(0, np.nan, inplace=True)
    jinkou_c.dropna(inplace=True)
    jinkou_c.to_csv(path + '\\' + col_dict[c] + '.csv', encoding='utf-8')


## 进出口数据的文件处理

path = 'E:\CBNB\BackTestSystem3\lib\data\operation_rate'
file_nm = 'E:\CBNB\BackTestSystem3\lib\data\operation_rate\开工率.xlsx'

col_dict = {'MEG国内开工率': 'MEG_DOMESTIC',
            'TA国内开工率': 'TA_DOMESTIC',
            'PE总开工率': 'PE_TOTAL',
            'PP总开工率': 'PP_TOTAL',
            'MA全国开工率': 'MA_DOMESTIC',
            'MA西北开工率': 'MA_NORTHWEST',
            'PVC整体开工率': 'PVC_TOTAL',
            'BU总开工率': 'BU_TOTAL'}
operation_rate = pd.read_excel(file_nm, index_col='日期')
operation_rate = operation_rate[col_dict.keys()]
for c in operation_rate.columns:
    operation_rate_c = operation_rate[[c]].copy()
    # operation_rate_c.replace(0, np.nan, inplace=True)
    operation_rate_c.dropna(inplace=True)
    operation_rate_c.to_csv(path + '\\' + col_dict[c] + '.csv', encoding='utf-8')

# ## 库存数据的文件处理
# path = 'E:\CBNB\BackTestSystem\lib\data\inventory_db'
# file_nm = u'E:\CBNB\BackTestSystem\lib\data\inventory_db\库存.xlsx'
#
# col_dict = {'PTA': 'PTA',
#             'MEG': 'MEG',
#             'LL': 'LL',
#             'PP': 'PP',
#             u'螺纹': 'RB',
#             u'热卷': 'HC',
#             u'甲醇': 'MA',
#             u'沥青': 'BU'}
#
# inventory_xls = pd.read_excel(file_nm, index_col='date')
# inventory_xls = inventory_xls[col_dict.keys()]
# for c in inventory_xls:
#     print c
#     file_c = os.path.join(path, '%s.csv' % col_dict[c])
#     if os.path.exists(file_c):
#         df_c = pd.read_csv(file_c, index_col='date', parse_dates=True)
#         df_c_new = inventory_xls[[c]]
#         df_c_new.rename(columns={c: col_dict[c]}, inplace=True)
#         # print df_c_new
#         # index_1 = df_c.index
#         # index_2 = df_c_new.index
#         # index_intersect = set(index_1).intersection(set(index_2))
#         # print index_1, index_2
#         # print index_intersect
#         # print col_dict[c], c
#         df_c = pd.merge(df_c, df_c_new, left_index=True, right_index=True, on=col_dict[c], how='outer')
#         df_c.to_csv(os.path.join(path, '%s.csv' % col_dict[c]))
#     else:
#         df_c = inventory_xls[[c]]
#         df_c.rename(columns={c: col_dict[c]}, inplace=True)
#         df_c.to_csv(os.path.join(path, '%s.csv' % col_dict[c]))






