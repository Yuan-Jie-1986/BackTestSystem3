from analysis.facbank import FacGenerator as fg
from analysis.factool import FactorAnalysis as fa
import pandas as pd
import matplotlib.pyplot as plt
import itertools as it
import os
import sys


fgObj = fg(start_date='20120101', end_date='20181231')

input_data_abs = ['cls', 'opn', 'high', 'low', 'volume', 'oi']
input_data_rel = ['rtn']
pairs_var_one_param = ['correlation', 'covariance']
single_var_no_params = ['rank', 'scale']
single_var_one_param = ['decay_linear', 'delay', 'delta', 'ts_argmax', 'ts_argmin', 'ts_max', 'ts_mean', 'ts_median',
                        'ts_min', 'ts_prod', 'ts_rank', 'ts_std', 'ts_sum']

if not os.path.exists('fac_total_result.csv'):
    fac_total = pd.DataFrame()
else:
    fac_total = pd.read_csv('fac_total_result.csv', index_col=0)

len1 = len(input_data_abs)
len2 = len(input_data_rel)
len3 = len(pairs_var_one_param)
len4 = len(single_var_no_params)
len5 = len(single_var_one_param)

total = len4 * len2
count = 1.
for op in single_var_no_params:
    for d in input_data_rel:
        prossess_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
        sys.stdout.write(prossess_str + u'【已完成%6.2f%%】' % (count * 100. / total))
        sys.stdout.write('\r')
        sys.stdout.flush()

        formula = '%s.%s(%s.%s)' % ('fgObj', op, 'fgObj', d)
        formula_df = pd.DataFrame()
        fac1 = eval(formula)
        faObj = fa(fac1, fgObj.cls)

        formula_df = formula_df.join(faObj.normal_ic(1, 60), how='outer')
        formula_df = formula_df.join(faObj.rank_ic(1, 60), how='outer')
        formula_df = formula_df.join(faObj.fac_rtn(3, 1, 60), how='outer')
        formula_df = formula_df.join(faObj.effective_num(), how='outer')
        # print '==============%s============' % formula
        formula_df.to_csv(os.path.join('analysis_result', '%s.csv' % formula))
        fac_total = pd.concat((fac_total, formula_df.mean().to_frame(name=formula).T))
        count += 1.

sys.stdout.write('\n')
sys.stdout.flush()

total = len5 * len2
count = 1.
for op in single_var_one_param:
    for d in input_data_rel:
        prossess_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
        sys.stdout.write(prossess_str + u'【已完成%6.2f%%】' % (count * 100. / total))
        sys.stdout.write('\r')
        sys.stdout.flush()

        formula_df = pd.DataFrame()
        try:
            formula = '%s.%s(%s.%s, 20, min_periods=15)' % ('fgObj', op, 'fgObj', d)
            fac1 = eval(formula)
        except TypeError:
            formula = '%s.%s(%s.%s, 20)' % ('fgObj', op, 'fgObj', d)
            fac1 = eval(formula)

        faObj = fa(fac1, fgObj.cls)
        formula_df = formula_df.join(faObj.normal_ic(1, 60), how='outer')
        formula_df = formula_df.join(faObj.rank_ic(1, 60), how='outer')
        formula_df = formula_df.join(faObj.fac_rtn(3, 1, 60), how='outer')
        formula_df = formula_df.join(faObj.effective_num(), how='outer')
        # print '==============%s============' % formula
        formula_df.to_csv(os.path.join('analysis_result', '%s.csv' % formula))
        fac_total = pd.concat((fac_total, formula_df.mean().to_frame(name=formula).T))
        count += 1.

sys.stdout.write('\n')
sys.stdout.flush()

total = len3 * len(list(it.combinations(input_data_rel + input_data_abs, 2)))
count = 1.
for op in pairs_var_one_param:
    for d1, d2 in it.combinations(input_data_rel + input_data_abs, 2):
        prossess_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
        sys.stdout.write(prossess_str + u'【已完成%6.2f%%】' % (count * 100. / total))
        sys.stdout.write('\r')
        sys.stdout.flush()

        formula = '%s.%s(%s.%s, %s.%s, %d, min_periods=%d)' % ('fgObj', op, 'fgObj', d1, 'fgObj', d2, 20, 15)
        formula_df = pd.DataFrame()
        fac1 = eval(formula)
        faObj = fa(fac1, fgObj.cls)
        formula_df = formula_df.join(faObj.normal_ic(ic_len=1, ir_len=60), how='outer')
        formula_df = formula_df.join(faObj.rank_ic(ic_len=1, ir_len=60), how='outer')
        formula_df = formula_df.join(faObj.fac_rtn(fac_num=3, rtn_len=1, ir_len=60), how='outer')
        formula_df = formula_df.join(faObj.effective_num(), how='outer')
        # print '==============%s============' % formula
        formula_df.to_csv(os.path.join('analysis_result', '%s.csv' % formula))
        fac_total = pd.concat((fac_total, formula_df.mean().to_frame(name=formula).T))
        count += 1.

sys.stdout.write('\n')
sys.stdout.flush()

fac_total.to_csv('fac_total_result.csv')
# fac_total.to_clipboard()






# fac1 = fgObj.ts_rank(fgObj.ts_sum(fgObj.rtn, period=20, min_periods=14), period=250, min_periods=200)
# faObj = fa(fac1, fgObj.cls)
# rank_ic = faObj.rank_ic()
# rank_ic.plot(grid=True)
# print rank_ic.describe()
# plt.show()
