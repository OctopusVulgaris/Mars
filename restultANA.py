# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import datetime
import time
import os
import argparse

'''
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
'''
def week():
    path = 'd:/tradelog/week/'
    logfiles = os.listdir(path)
    dfs = []
    inittotal = 3000000
    cnt=0
    for file in logfiles:
        if file.find('h_') == 0:
            params = file.strip('h_.csv').split('_')

            df = pd.read_csv(path + file,parse_dates=['date'], names=['date', 'code', 'buyprc', 'buyhfq', 'vol', 'histhigh', 'daystosell', 'amo', 'cash', 'total'])
            if df.empty:
                continue
            df['hh'] = df.total.expanding().max()
            df['maxdd'] = 1 - (df.total / df.hh).min()
            df = df.set_index('date')
            df = df[['total', 'maxdd']]
            # 15 params
            # [0,1,2,3] g_sellpoint | g_maxfallback | g_DELAYSELL | g_maxHold
            # [4,5] onedaystopprofit | onedaystoplose
            # [6,7] pamopctflag | pclspctflag
            # [8,9,10] pamolessmax20 | phighlessma10 | ma10lessma20
            # [11] lowlessphigh
            # [12,13,14] maxmin20greater | maxmin20less | phighlow20less
            # [15] startdate , 2008-1-1/1199116800
            df['sellpoint'] = params[0]
            df['maxfallback'] = params[1]
            df['delaysell'] = params[2]
            df['maxHold'] = params[3]
            df['onedaystopprofit'] = params[4]
            df['onedaystoplose'] = params[5]
            df['pamopctflag'] = params[6]
            df['pclspctflag'] = params[7]
            df['pamolessmax20'] = params[8]
            df['phighlessma10'] = params[9]
            df['ma10lessma20'] = params[10]
            df['lowlessphigh'] = params[11]
            df['maxmin20greater'] = params[12]
            df['maxmin20less'] = params[13]
            df['phighlow20less'] = params[14]
            startdate = time.localtime(int(params[15][:-2]))
            df['startdate'] = time.strftime('%Y-%m-%d', startdate)
            df['years'] = float(df.index[-1].year*12 + df.index[-1].month - startdate.tm_year*12 - startdate.tm_mon) / 12
            df['skipbuypoint'] = params[16]
            df['earningrate'] = float(df.total.iloc[-1]) / df.total.iloc[0]
            df['earningperyear'] = np.power(df.earningrate.iloc[-1], 1.0 / df.years.iloc[-1]) - 1
            df = df.tail(1)
            dfs.append(df)
            cnt+=1
            print(cnt)
    df = pd.concat(dfs)
    df.to_csv('d:/tradelog/week/all_h.csv')

def pttp():
    path = 'd:/tradelog/pttp/'
    logfiles = os.listdir(path)
    dfs = []
    inittotal = 3000000
    cnt=0
    for file in logfiles:
        if file.find('h_') == 0:
            params = file.strip('h_.csv').split('_')

            df = pd.read_csv(path + file,parse_dates=['date'], names=['date', 'code', 'buyprc', 'buyhfq', 'vol', 'histhigh', 'daystosell', 'amo', 'cash', 'total'])
            if df.empty:
                continue
            df['hh'] = df.total.expanding().max()
            df['maxdd'] = 1 - (df.total / df.hh).min()
            df = df.set_index('date')
            df = df[['total', 'maxdd']]
            '''
            g_maxfallback = activeparam[0]
            epsflag = activeparam[1]
            ocrateflag = activeparam[2]
            buyselladj = activeparam[3]
            g_DELAYNOSELL = int64_t(activeparam[4])
            startdate = int64_t(activeparam[5]);
            '''
            df['maxfallback'] = params[0]
            df['epsflag'] = params[1]
            df['ocrateflag'] = params[2]
            df['buyselladj'] = params[3]
            df['DELAYNOSELL'] = params[4]
            startdate = time.localtime(int(params[5][:-2]))
            df['startdate'] = time.strftime('%Y-%m-%d', startdate)
            df['years'] = float(df.index[-1].year*12 + df.index[-1].month - startdate.tm_year*12 - startdate.tm_mon) / 12
            df['earningrate'] = float(df.total.iloc[-1]) / df.total.iloc[0]
            df['earningperyear'] = np.power(df.earningrate.iloc[-1], 1.0 / df.years.iloc[-1]) - 1
            df = df.tail(1)
            dfs.append(df)
            cnt += 1
            print(cnt)
    df = pd.concat(dfs)
    df.to_csv('d:/tradelog/pttp/all_h.csv')

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str)

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']


    if (type == 'week'):
        week()
    elif (type == 'pttp'):
        pttp()
