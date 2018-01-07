# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
import subprocess as sp
import sys
from utility import calcXdayR, calcXdayRmax, calcXdayRmin
def nor(x):
    return (x-x.mean())/x.std()

def Normalize(x):
    x = x[x.open > 0]
    n = x.amo / x.tradeablecap
    n = pd.DataFrame(n)
    n['high'] = x.high * x.hfqratio
    n['low'] = x.low * x.hfqratio
    n['open'] = x.open * x.hfqratio
    n['close'] = x.close * x.hfqratio
    n.columns = ['tnovrate', 'high', 'low', 'open', 'close']
    n['tr'] = n.tnovrate.expanding().apply(lambda x : nor(x)[-1])
    n['op'] = n.open.expanding().apply(lambda x: nor(x)[-1])
    n['cl'] = n.close.expanding().apply(lambda x: nor(x)[-1])
    n['hi'] = n.high.expanding().apply(lambda x: nor(x)[-1])
    n['lo'] = n.low.expanding().apply(lambda x: nor(x)[-1])
    n['opct'] = n.open.pct_change() + 1
    return n

df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5','day', where='date > \'2006-12-1\'')

a = df.groupby(level=0, group_keys=False).apply(Normalize)

r = a.tr.groupby('code', group_keys=False)
a['tr10'] = r.rolling(window=10).mean()
a['tr20'] = r.rolling(window=20).mean()
a['tr30'] = r.rolling(window=30).mean()
a['tr60'] = r.rolling(window=60).mean()

r = a.high.groupby(level=0, group_keys=False)
a['hstd30'] = r.rolling(window=30).std(ddof=0)
a['hstd60'] = r.rolling(window=60).std(ddof=0)
a['hstd90'] = r.rolling(window=90).std(ddof=0)


r = a.low.groupby(level=0, group_keys=False)
a['lstd30'] = r.rolling(window=30).std(ddof=0)
a['lstd60'] = r.rolling(window=60).std(ddof=0)
a['lstd90'] = r.rolling(window=90).std(ddof=0)


a['omean30'] = a.open.groupby(level=0, group_keys=False).rolling(window=30).mean()
a['cmean30'] = a.close.groupby(level=0, group_keys=False).rolling(window=30).mean()
a['hmax30'] = a.high.groupby(level=0, group_keys=False).rolling(window=30).max()
a['lmin30'] = a.low.groupby(level=0, group_keys=False).rolling(window=30).min()

a['lf'] = (a.hmax30 - a.lmin30) / (a.omean30 + a.cmean30) / 2

a['avp'] = (a.open + a.high + a.low + a.close) / 4
a['avp20'] = a.avp.groupby(level=0, group_keys=False).rolling(window=20).std(ddof=0)
a['avp30'] = a.avp.groupby(level=0, group_keys=False).rolling(window=30).std(ddof=0)
a['avp60'] = a.avp.groupby(level=0, group_keys=False).rolling(window=60).std(ddof=0)

a['R'] = a.opct.groupby(level=0).cumprod()





r = a.R.groupby(level=0)
a['R30'] = r.apply(calcXdayR, days=30)
a['R60'] = r.apply(calcXdayR, days=60)
a['R90'] = r.apply(calcXdayR, days=90)
a['R180'] = r.apply(calcXdayR, days=180)
a['R270'] = r.apply(calcXdayR, days=270)
a['R360'] = r.apply(calcXdayR, days=360)
a['Rmax30'] = r.apply(calcXdayRmax, days=30)
a['Rmax60'] = r.apply(calcXdayRmax, days=60)
a['Rmax90'] = r.apply(calcXdayRmax, days=90)
a['Rmax180'] = r.apply(calcXdayRmax, days=180)
a['Rmax270'] = r.apply(calcXdayRmax, days=270)
a['Rmax360'] = r.apply(calcXdayRmax, days=360)
a['Rmin30'] = r.apply(calcXdayRmin, days=30)
a['Rmin60'] = r.apply(calcXdayRmin, days=60)
a['Rmin90'] = r.apply(calcXdayRmin, days=90)
a['Rmin180'] = r.apply(calcXdayRmin, days=180)
a['Rmin270'] = r.apply(calcXdayRmin, days=270)
a['Rmin360'] = r.apply(calcXdayRmin, days=360)


b = a[(a.hstd30 < 0.2) & (a.lf < 0.08) & (a.tr30 < -0.6)]


def toMonth(x):
    x = x.reset_index(level=0, drop=True)
    return x.groupby(pd.TimeGrouper('M')).last()

c = b.groupby('code').apply(toMonth).dropna()


