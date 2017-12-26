# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
import subprocess as sp
import sys
wdf = pd.read_hdf('d:/hdf5_data/wdf.hdf').swaplevel().sort_index()

day = 4
x = pd.DataFrame()
x['x'] = wdf.ma5 - wdf.ma10
x['s'] = x.x.groupby(level=0).rolling(window=day).std().reset_index(level=0, drop=True)
x['m'] = x.x.groupby(level=0).rolling(window=day).mean().reset_index(level=0, drop=True)
x['mp'] = x.m / wdf.ma10

a = wdf[['ma5s3','ma10s3']].groupby(level=0).rolling(window=day).mean().reset_index(level=0)
b = wdf[['ma5s3','ma10s3']].groupby(level=0).rolling(window=day).std().reset_index(level=0)
c = wdf[['ma5s3','ma10s3']].groupby(level=0).rolling(window=day).min().reset_index(level=0)
d = wdf.close-wdf.ma10
d = d.groupby(level=0).rolling(window=day).min().reset_index(0)
d.columns=['code','m']
e = wdf.ma5s3s3.abs().groupby(level=0).rolling(window=day).max().reset_index(level=0)


wdf[(abs(a.ma5s3-a.ma10s3)/(a.ma5s3+a.ma10s3) < 0.05) & (b.ma5s3<0.05)
&(b.ma10s3<0.05)&(c.ma5s3>0)&(c.ma10s3>0)&(d.m>0)&(e.ma5s3s3<0.4)][['ma5s3', 'ma10s3','ma20s3']]

