# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
import subprocess as sp
import sys

def reconnect():
    sp.call('rasdial 宽带连接 /disconnect', stdout=sys.stdout)
    time.sleep(1)
    sp.call('rasdial 宽带连接 *63530620 040731', stdout=sys.stdout)
'''
def subx(x):
    return x[1] - x[0]

df = pd.read_csv('D:\\HDF5_Data\\fundmental\\601198.csv', encoding='gbk', index_col=u'报告日期', parse_dates=True).sort_index()
a = df.iloc[:,0].groupby(df.index.year).rolling(window=2).apply(subx).reset_index(level=0, drop=True).combine_first(df.iloc[:,0])
a.rolling(window=4).sum().combine_first(a)
'''
trade_type_dic = {
    '买盘' : 1,
    '卖盘' : -1,
    '中性盘' : 0
}

'''
all = all[all.open > 0]
grouped = all.groupby(['code', pd.Grouper(freq='1M', level=1)])
s =grouped.std()
s['opendelta'] = ((grouped.max()-grouped.min())/grouped.min()).open
v = grouped.vol.mean()
vp = v.groupby(level=0).pct_change()
s.vol = vp
s = s.dropna()
r = s[(s.open < 0.1) & (s.opendelta < 0.05) & (s.vol < -0.3)]

'''

ohlc_dict = {
    'open':'first',
    'high':'max',
    'low':'min',
    'close': 'last',
    'vol': 'sum',
    'amo': 'sum'
}

#df.groupby(level=0).resample('W', level=1).apply(ohlc_dict)
#df.groupby(level=0).resample('W', level=1).ohlc()

df = pd.read_hdf('d:/HDF5_Data/buylow_sellhigh_tmp.hdf', 'day', columns=['icode', 'idate', 'open', 'high', 'low', 'close', 'stflag', 'hfqratio']).swaplevel().sort_index()
hfq = pd.DataFrame()
hfq['open'] = df.open * df.hfqratio
hfq['high'] = df.high * df.hfqratio
hfq['low'] = df.low * df.hfqratio
hfq['close'] = df.close * df.hfqratio


wdf = pd.DataFrame()
gp = hfq.groupby(level=0)
wdf['open'] = gp.open.resample('W', level=1).first()
wdf['high'] = gp.high.resample('W', level=1).max()
wdf['low'] = gp.low.resample('W', level=1).min()
wdf['close'] = gp.close.resample('W', level=1).last()
#wdf['hfq'] = gp.hfq.resample('W', level=1).last()
gp = wdf.groupby(level=0)
wdf['ma10'] = gp.close.rolling(window=10).mean()
wdf['ma20'] = gp.close.rolling(window=20).mean()
wdf['ma30'] = gp.close.rolling(window=30).mean()
wdf['hfq'] = 1

wdf.to_hdf('d:/HDF5_Data/wdf.hdf', 'week', mode='w', format='t', complib='blosc')





