# 关于data文件夹中的脚本的说明文档

* supplement_db文件夹中的文件是现货的价格数据，需要每天更新。文件夹中的现货价格.xlsx的文件需要手动打开，每天更新到最近的数据。
* spot_xls_2_csv.py是将现货价格.xlsx的数据按照品种生成一个一个的csv文件。
* find_main_contract.py是将数据库中的wind的主力合约增加新的字段specific_contract和switch_contract。该脚本需要每回数据更新后再跑。
* profit_rate.py是根据各品种的利润计算公式生成的盘面利润因子，并导入了数据库
* cmd_index.py是根据各品种的合约生成的指数，命名规则商品代码+888.+交易所代码，这里有个问题是，要时刻关注该品种的合约是否完全抓取到数据库中。也就是要定期更新数据库中Information的那张表。


