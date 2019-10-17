import tushare as ts
import time
import pandas as pd
import numpy as np
import threading
#from threading import Thread, Lock, current_thread
# 此版本使用多线程
ts.set_token('20f64f9be7cff644f8a71b99a7eb4853b2c37e5f72e6128f2055fba4')
pro = ts.pro_api()
class Stock_data():
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

    def get_daily_data(self, stock_list, L):
        lock = threading.Lock()
        while True:
            if stock_list == []:
                break
            lock.acquire()
            ts_code = stock_list.pop()
            lock.release()  
            print(f'{threading.current_thread().getName()}获取{ts_code}了，准备获取数据')
            stock_daily_lists = pro.daily(ts_code=ts_code, start_date=self.up_time, end_date=self.end_date)
            df = pro.daily_basic(ts_code=ts_code, start_data=self.up_time, end_date=self.end_date)
            data_list = pd.merge(stock_daily_lists, df, on=['ts_code', 'trade_date', 'close'])
            print(f'{threading.current_thread().getName()}准备存放{ts_code}的数据')
            lock.acquire()
            L.append(data_list)
            print(f'{ts_code} is OK!')
            lock.release()
        return L
    def thread_pool(self, stock_list):
        L = []
        thread_name = ['1号线程', '2号线程', '3号线程', '4号线程', '5号线程']
        for name in thread_name:
            print(f'-------{name}启动了--------')
            td = threading.Thread(target=self.get_daily_data, name=name, args=(stock_list, L))
            td.start()
        td.join()         
        print('线程结束') 
        return L
    def data_group(self):
        print('1')
        stock_list = self.get_stock_list()['ts_code']
        print('2')
        stock_list = list(stock_list)
        print('开始调用进程')
        L = self.thread_pool(stock_list)
        print('结束')
        return pd.concat(L)
if __name__ == '__main__':   
    lists = Stock_data(up_time='20100101', end_date='20191016').data_group()
    lists.to_csv('stock_data.csv')
