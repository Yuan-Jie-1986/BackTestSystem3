# 关于data文件夹中的脚本的说明文档

* supplement_db文件夹中的文件是现货的价格数据，需要每天更新。文件夹中的现货价格.xlsx的文件需要手动打开，每天更新到最近的数据。
* spot_xls_2_csv.py是将现货价格.xlsx的数据按照品种生成一个一个的csv文件。
* find_main_contract.py是将数据库中的wind的主力合约增加新的字段specific_contract和switch_contract。该脚本需要每回数据更新后再跑。

