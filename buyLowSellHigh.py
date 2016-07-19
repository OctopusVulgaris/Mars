# -*- coding:utf-8 -*-

import pandas as pd
import sqlalchemy as sa
import datetime
import numpy as np
import ctypes as ct
import time
import talib
from utility import round_series, get_realtime_all_st


import logging
import talib as ta


max_holdings = 10
holding_cnt = 0
holdings = {}
summits = {}
to_be_sell = []
cash = 100000.0
poolsize = 300

st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'


def GetTotalCapIndex(x):
    x = x.sort_values('ptotalcap')
    x = x.head(int(len(x) / 10))
    return x.ptotalcap.sum() / 100000


def sort(x):

    x = x.sort_values('ptotalcap', ascending=True)

    # check st, final true is ready to buy
    x['stflag'] = 0
    x.loc[x.name.str.contains(st_pattern), 'stflag'] = 1
    x['buyflag'] = x.stflag < 1
    # open on this day
    x['buyflag'] = x.buyflag & (x.open > 0.01)
    # open low , but not over prev low limit and don't reach today low limit
    # if halt yesterday, low == 0, this case don't buy
    x['buyflag'] = x.buyflag & (x.open < x.plow)
    x['buyflag'] = x.buyflag & (x.open > x.lowlimit)
    x['buyflag'] = x.buyflag & (x.open > x.plowlimit)
    x['buyflag'] = x.buyflag & (x.hfqratio >= 1)

    return x


def calc(x):

    valid = x[x.open > 0.01]

    validLen = len(valid)
    validyesterday = valid.iloc[:validLen - 1]

    valid['phfqratio'] = 1
    valid['phfqratio'].iloc[1:] = validyesterday.hfqratio.values
    factor = valid.phfqratio.iloc[1:] / valid.hfqratio.iloc[1:]
    valid['pclose'] = 0
    valid['pclose'].iloc[1:] = validyesterday.close.values * factor
    valid['pclose'] = round_series(valid.pclose)

    valid['plow'] = 0
    valid['plow'].iloc[1:] = validyesterday.low.values * factor
    valid['plow'] = round_series(valid.plow)

    valid['phigh'] = 0
    valid['phigh'].iloc[1:] = validyesterday.high.values * factor
    valid['phigh'] = round_series(valid.phigh)

    valid['lowlimit'] = valid.pclose * 0.9
    valid['lowlimit'] = round_series(valid.lowlimit)

    valid['highlimit'] = valid.pclose * 1.1
    valid['highlimit'] = round_series(valid.highlimit)

    valid['plowlimit'] = 0
    valid['plowlimit'].iloc[1:] = valid.lowlimit.values[:validLen - 1] * factor
    valid['plowlimit'] = round_series(valid.plowlimit)

    valid = valid.reindex(x.index)

    validLen = len(x)
    # on day data, value exist no matter haltx
    valid['ptotalcap'] = 0
    valid['ptotalcap'].iloc[1:] = x.totalcap.values[:validLen - 1]

    valid['name'] = x['name']
    valid['totalcap'] = x.totalcap
    valid['hfqratio'] = x.hfqratio
    valid['phfqratio'].iloc[1:] = valid.hfqratio.values[:validLen - 1]
    valid['pclose'] = valid.pclose.fillna(method='ffill')
    valid = valid.fillna(0)

    return valid


def csvtoHDF():
    t1 = datetime.datetime.now()
    print 'reading...'
    aa = pd.read_csv('d:\\daily\\all_consolidate.csv', index_col='date', usecols=['code', 'date', 'name', 'close', 'high', 'low', 'open', 'vol', 'amo', 'totalcap', 'hfqratio'], parse_dates= True, chunksize= 500000, dtype={'code': np.str})
    df = pd.concat(aa)

    print len(df)

    df.sort_index(inplace=True)
    print datetime.datetime.now() - t1

    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\dailydata.hdf','day',mode='w', format='t', complib='blosc')

    print len(df)
    print datetime.datetime.now() - t1

def sqltoHDF():
    t1 = datetime.datetime.now()
    print 'reading...'
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
    aa = pd.read_sql_table('dailydata', engine, index_col='date', columns=['code', 'date', 'name', 'close', 'high', 'low', 'open', 'vol', 'amo', 'totalcap', 'hfqratio'], parse_dates= True, chunksize= 500000)
    df = pd.concat(aa)

    print len(df)

    df.sort_index(inplace=True)
    print datetime.datetime.now() - t1
    df['nameutf'] = 'utf8'
    df['codeutf'] = 'utf8'
    df.nameutf = df.name.str.encode('utf-8')
    df.codeutf = df.code.str.encode('utf-8')
    del df['name']
    del df['code']
    df.columns = ['close', 'high', 'low', 'open', 'vol', 'amo', 'totalcap', 'hfqratio', 'name', 'code']

    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\dailydata.hdf','day',mode='w', format='t', complib='blosc')

    print len(df)
    print datetime.datetime.now() - t1

def ComputeCustomIndex(df):
    #t1 = datetime.datetime.now()
    #df = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day')
    #df = df[df.code.str.contains(ashare_pattern)]

    #print datetime.datetime.now()- t1
    groupbydate = df.groupby(level=0)
    myindex = pd.DataFrame()
    myindex['trdprc'] = groupbydate.apply(GetTotalCapIndex)
    myindex['ma9'] = talib.MA(myindex.trdprc.values, timeperiod=9)
    myindex['ma12'] = talib.MA(myindex.trdprc.values, timeperiod=12)
    myindex['ma60'] = talib.MA(myindex.trdprc.values, timeperiod=60)
    myindex['ma256'] = talib.MA(myindex.trdprc.values, timeperiod=256)
    myindex = myindex.fillna(0)

    myindex.to_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day', mode='w', format='f', complib='blosc')

    #print datetime.datetime.now() - t1

def prepareMediateFile():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5','dayk', columns=['close', 'high', 'low', 'open', 'totalcap', 'name', 'hfqratio'], where='date > \'2006-5-1\'')
    #df = df[df.code.str.contains(ashare_pattern)]


    print len(df)

    df.sort_index(inplace=True)
    print datetime.datetime.now() - t1

    groupbycode = df.groupby(level=0)

    print 'calculating...'
    df = groupbycode.apply(calc)

    lastday = df.index.get_level_values(1)[-1]
    tomorrow = df.loc(axis=0)[:, lastday].reset_index()

    # following values doesn't need adjust by hfqratio
    tomorrow.ptotalcap = tomorrow.totalcap
    tomorrow.phfqratio = tomorrow.hfqratio
    # following value need fill by real time value in the morning
    tomorrow.date = datetime.date(2050, 1, 1)
    tomorrow.open = 0
    tomorrow.hfqratio = 1
    tomorrow.stflag = 0
    tomorrow.high = 0
    tomorrow.low = 0

    # followiwng value still in yesterday ratio, need adjust by realtime hfqratio in the morning
    tomorrow.pclose = tomorrow.close
    tomorrow.plowlimit = tomorrow.lowlimit
    tomorrow.plow = tomorrow.low
    tomorrow.phigh = tomorrow.high
    tomorrow.lowlimit = tomorrow.pclose * 0.9
    tomorrow.highlimit = tomorrow.pclose * 1.1

    df = df.append(tomorrow.set_index(['code', 'date']))

    print 'switching index...'
    df = df[df.name.str.startswith('N') != True]

    df = df.reset_index()
    df = df.set_index(['date', 'code'], drop=False)
    df.date = df.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    df.code = df.code.apply(lambda x: np.int64(x))
    df = df.rename(columns={'date': 'idate', 'code': 'icode'})
    df = df.sort_index()
    print 'sorting...'

    groupbydate = df.groupby(level=0)
    df = groupbydate.apply(sort)

    df = df.reset_index(level=0, drop=True)

    ComputeCustomIndex(df)
    print datetime.datetime.now() - t1
    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf','day',mode='w', format='t', complib='blosc')

    print len(df)
    print datetime.datetime.now() - t1



def Processing():
    print time.clock()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', where='date > \'2008-1-6\'')
    index = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    index = index.fillna(0)
    index = index.loc['2008-1-1':]

    c_double_p = ct.POINTER(ct.c_double)
    #set log level
    setloglevel = ct.cdll.LoadLibrary('d:\\BLSH.dll').setloglevel
    setloglevel.argtypes = [ct.c_int64]
    setloglevel(3)
    # set index
    setindex = ct.cdll.LoadLibrary('d:\\BLSH.dll').setindex
    setindex.restype = ct.c_int64
    setindex.argtypes = [ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_int]

    cdate = index.index.to_series().apply(lambda x: np.int64(time.mktime(x.timetuple()))).get_values().ctypes.data_as(ct.c_void_p)
    cprc = index.trdprc.get_values().ctypes.data_as(c_double_p)
    cma1 = index.ma9.get_values().ctypes.data_as(c_double_p)
    cma2 = index.ma12.get_values().ctypes.data_as(c_double_p)
    cma3 = index.ma60.get_values().ctypes.data_as(c_double_p)
    cma4 = index.ma256.get_values().ctypes.data_as(c_double_p)

    setindex(cdate, cprc, cma1, cma2, cma3, cma4, len(index))

    # process
    process = ct.cdll.LoadLibrary('d:\\BLSH.dll').process
    setindex.restype = ct.c_int64
    process.argtypes = [ct.c_void_p, ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_void_p, ct.c_int]

    #ti = ct.cdll.LoadLibrary('d:\\BLSH.dll').testint
    #td = ct.cdll.LoadLibrary('d:\\BLSH.dll').testdouble
    #tui = ct.cdll.LoadLibrary('d:\\BLSH.dll').testuint
    #ti.argtypes = [ct.c_void_p, ct.c_int]
    #td.argtypes = [c_double_p, ct.c_int]
    #tui.argtypes = [ct.c_void_p, ct.c_int]

    print time.clock()

    cdate = df.idate.get_values().ctypes.data_as(ct.c_void_p)
    ccode = df.icode.get_values().ctypes.data_as(ct.c_void_p)
    cpclose = df.pclose.get_values().ctypes.data_as(c_double_p)
    cphigh = df.phigh.get_values().ctypes.data_as(c_double_p)
    cplow = df.plow.get_values().ctypes.data_as(c_double_p)
    cplowlimit = df.plowlimit.get_values().ctypes.data_as(c_double_p)
    copen = df.open.get_values().ctypes.data_as(c_double_p)
    chighlimit = df.highlimit.get_values().ctypes.data_as(c_double_p)
    clowlimit = df.lowlimit.get_values().ctypes.data_as(c_double_p)
    chfqratio = df.hfqratio.get_values().ctypes.data_as(c_double_p)
    cstflag = df.stflag.get_values().ctypes.data_as(ct.c_void_p)

    for i in range(0, 1):
        ret = process(cdate, ccode, cpclose, cphigh, cplow, cplowlimit, copen, chighlimit, clowlimit, chfqratio, cstflag, len(df))


    print time.clock()



prepareMediateFile()
#Processing()

