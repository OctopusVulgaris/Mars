# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import datetime
import time
import os
path = 'd:/tradelog/FTCOR/'
dfs = []
logfiles = os.listdir(path)
for file in logfiles:
    if file.find('FTCOR_p_') == 0:
        params = file.strip('FTCOR_p_.csv').split('_')
        df = pd.read_csv(path + file)
        d = df.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9])
        d = d[['rate', 'daterange', 'rateperyear']]
        d = d.T
        d.index.set_names('static', inplace=True)
        d['buyrate'] = params[0]
        d['sellrate'] = params[1]
        d['fallback'] = params[2]
        d['highfallback'] = params[3]
        d['finishrate'] = params[4]
        d['startdate'] = params[5]
        d = d.set_index([d.index, 'buyrate', 'sellrate', 'fallback', 'highfallback', 'finishrate', 'startdate'])
        dfs.append(d)

df = pd.concat(dfs)
df.to_csv('d:/tradelog/FTCOR/all.csv')

dfs = []
for file in logfiles:
    if file.find('FTCOR_h_') == 0:
        params = file.strip('FTCOR_h_.csv').split('_')
        df = pd.read_csv(path + file, parse_dates=[0])
        df['hh'] = df.total.expanding().max()
        df['maxdd'] = 1 - (df.total / df.hh).min()
        df = df.set_index('date')
        df = df[['total', 'maxdd']]
        df['buyrate'] = params[0]
        df['sellrate'] = params[1]
        df['fallback'] = params[2]
        df['highfallback'] = params[3]
        df['finishrate'] = params[4]
        df['startdate'] = params[5]
        df['years'] = (df.index[-1].year - df.index[0].year) + float(df.index[-1].month - df.index[0].month) / 10
        df['earningrate'] = float(df.total.iloc[-1]) / df.total.iloc[0]
        df['earningperyear'] = np.power(df.earningrate.iloc[-1], 1.0 / df.years.iloc[-1] ) - 1
        df = df.set_index([df.index, 'buyrate', 'sellrate', 'fallback', 'highfallback', 'finishrate', 'startdate']).tail(1)
        dfs.append(df)

df = pd.concat(dfs)
df.to_csv('d:/tradelog/FTCOR/all_h.csv')

dfs = []
logfiles = os.listdir(path)
for file in logfiles:
    if file.find('FTCOR_s_') == 0:
        params = file.strip('FTCOR_s_.csv').split('_')
        df = pd.read_csv(path + file)
        d = df.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9])
        d = d[['rate', 'daterange', 'rateperyear']]
        d = d.T
        d.index.set_names('static', inplace=True)
        d['buyrate'] = params[0]
        d['sellrate'] = params[1]
        d['fallback'] = params[2]
        d['highfallback'] = params[3]
        d['finishrate'] = params[4]
        d = d.set_index([d.index, 'buyrate', 'sellrate', 'fallback', 'highfallback', 'finishrate'])
        dfs.append(d)

df = pd.concat(dfs)
df.to_csv('d:/tradelog/FTCOR/all.csv')
