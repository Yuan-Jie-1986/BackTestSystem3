
import yaml
from BackTestSystem3.lib.data.Unused.base import DataSaving
import requests

# 切记：路透的数据更新需要等到九点之后，否则可能会更新到还没有停止交易的合约。
# 对于国外的合约的数据，一定要检查合约的交易时间以及与北京时间的时差问题

f = open('config.yaml', encoding='utf-8')
res = yaml.load(f, Loader=yaml.FullLoader)
host = res['host']
port = res['port']
usr = res['user']
pwd = res['pwd']
db = res['db_name']
logpath = res['log_path']

ds_obj = DataSaving(host=host, port=port, usr=usr, pwd=pwd, db=db, log_path=logpath)

cols = res['collection']

error_list = []

for col in cols:
    col_content = res[col]
    print(col_content)
    for cc in col_content:
        try:
            func = cc.pop('func')
            cmd_list = cc.pop('cmd')
            if isinstance(cmd_list, list):
                for c in cmd_list:
                    getattr(ds_obj, func)(col, c, **cc)
            else:
                getattr(ds_obj, func)(col, cmd_list, **cc)
        except requests.exceptions.MissingSchema as e:
            print(e)
            error_list.append(e)

f.close()

for err in error_list:
    print(err)
