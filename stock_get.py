import tushare as ts
import time
import pandas as pd
import numpy as np

ts.set_token('20f64f9be7cff644f8a71b99a7eb4853b2c37e5f72e6128f2055fba4')
pro = ts.pro_api()
class Stock_data(object):
    def __init__(self, up_time, end_date):
        self.up_time = up_time
        self.end_date = end_date

    def get_stock_list(self):
        # 获取的数据是DataFrame格式的表
        stock_lists = pro.stock_basic(list_status='L', exchange='', fields='ts_code, name, industry, list_date')
        # 根据上市时间筛选从2015-01-01开始上市的股票数据
        df = pd.DataFrame(stock_lists)
        stock_lists = df[df['list_date']<self.up_time]
        return stock_lists

    def get_daily_data(self, ts_code):
        stock_daily_lists = pro.daily(ts_code=ts_code, start_date=self.up_time, end_date=self.end_date)
        df = pro.daily_basic(ts_code=ts_code, start_data=self.up_time, end_date=self.end_date)
        return pd.merge(stock_daily_lists, df, on=['ts_code', 'trade_date', 'close'])

    def data_group(self):
        stock_lists = self.get_stock_list()['ts_code']
        L = []
        for stock_code in stock_lists:
            stock_daily = self.get_daily_data(stock_code)
            L.append(stock_daily)
            print(f"{stock_code}'s data is OK!!")
        print('进行数据合并。')
        return pd.concat(L)

if __name__ == '__main__':   
    lists = Stock_data(up_time='20100101', end_date='20191016').data_group()
    lists.to_csv('stock_data.csv')
