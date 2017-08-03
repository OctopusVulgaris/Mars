# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
'''
def subx(x):
    return x[1] - x[0]

df = pd.read_csv('D:\\HDF5_Data\\fundmental\\601198.csv', encoding='gbk', index_col=u'报告日期', parse_dates=True).sort_index()
a = df.iloc[:,0].groupby(df.index.year).rolling(window=2).apply(subx).reset_index(level=0, drop=True).combine_first(df.iloc[:,0])
a.rolling(window=4).sum().combine_first(a)
'''
for i in range(1000):
    tick = ts.get_tick_data('000001', date='2015-07-31', retry_count=10, src='sn')
    print(i)
    time.sleep(2)