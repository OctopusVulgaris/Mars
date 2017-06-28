# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np

def subx(x):
    return x[1] - x[0]

df = pd.read_csv('D:\\HDF5_Data\\fundmental\\601198.csv', encoding='gbk', index_col=u'报告日期', parse_dates=True).sort_index()
a = df.iloc[:,0].groupby(df.index.year).rolling(window=2).apply(subx).reset_index(level=0, drop=True).combine_first(df.iloc[:,0])
a.rolling(window=4).sum().combine_first(a)