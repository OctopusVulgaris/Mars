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

st_pattern = r'^S|^\*|退市'
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
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5','dayk', columns=['close', 'high', 'low', 'open', 'totalcap', 'tradeablecap', 'name', 'hfqratio'], where='date > \'2006-5-1\'')
    #df = df[df.code.str.contains(ashare_pattern)]


    print len(df)
    df = df[df.tradeablecap > 0]
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


def doProcessing(df, loglevel):
    print time.clock()
    print 'reading index...'
    index = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    index = index.fillna(0)
    index = index.loc['2008-1-1':]

    c_double_p = ct.POINTER(ct.c_double)
    BLSH = ct.cdll.LoadLibrary('d:\\BLSH.dll')
    # set log level
    BLSH.setloglevel.argtypes = [ct.c_int64]
    BLSH.setloglevel(loglevel)
    # set index
    BLSH.setindex.restype = ct.c_int64
    BLSH.setindex.argtypes = [ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_int]

    cdate = index.index.to_series().apply(lambda x: np.int64(time.mktime(x.timetuple()))).get_values().ctypes.data_as(ct.c_void_p)
    cprc = index.trdprc.get_values().ctypes.data_as(c_double_p)
    cma1 = index.ma9.get_values().ctypes.data_as(c_double_p)
    cma2 = index.ma12.get_values().ctypes.data_as(c_double_p)
    cma3 = index.ma60.get_values().ctypes.data_as(c_double_p)
    cma4 = index.ma256.get_values().ctypes.data_as(c_double_p)

    BLSH.setindex(cdate, cprc, cma1, cma2, cma3, cma4, len(index))

    # process
    BLSH.process.restype = ct.c_int64
    BLSH.process.argtypes = [ct.c_void_p, ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_void_p, ct.c_int]
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

    ret = BLSH.process(cdate, ccode, cpclose, cphigh, cplow, cplowlimit, copen, chighlimit, clowlimit, chfqratio, cstflag, len(df))

        # ti = ct.cdll.LoadLibrary('d:\\BLSH.dll').testint
        # td = ct.cdll.LoadLibrary('d:\\BLSH.dll').testdouble
        # tui = ct.cdll.LoadLibrary('d:\\BLSH.dll').testuint
        # ti.argtypes = [ct.c_void_p, ct.c_int]
        # td.argtypes = [c_double_p, ct.c_int]
        # tui.argtypes = [ct.c_void_p, ct.c_int]
    print time.clock()

def regressionTest():
    print time.clock()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', where='date > \'2008-1-6\'')
    doProcessing(df, 1)

def morningTrade():
    print time.clock()
    logging.info('retrieving today all...')
    realtime = pd.DataFrame()
    retry = 0
    while not get and retry < 15:
        try:
            retry += 1
            # today = get_today_all()
            realtime = get_realtime_all_st()
            realtime = realtime.set_index('code')
            if realtime.index.is_unique and len(realtime[realtime.open > 0]) > 500:
                get = True
        except Exception:
            logging.error('retrying...')
    print time.clock()
    print 'reading temp file...'
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', where='date = \'2050-1-1\'')

    realtime = realtime[realtime.prev_close > 0]
    df = df.reset_index(0)
    df.reindex(realtime.index, fill_value=0)

    df.date = datetime.date.today()
    df.open = realtime.open
    df.hfqratio = df.phfqratio * df.pclose / realtime.prev_close
    df.loc[realtime.name.str.contains(st_pattern), 'stflag'] = 1

    factor = df.phfqratio / df.hfqratio
    df.pclose = round_series(df.pclose * factor)
    df.plowlimit = round_series(df.plowlimit * factor)
    df.plow = round_series(df.plow * factor)
    df.phigh = round_series(df.phigh * factor)
    df.lowlimit = round_series(df.lowlimit * factor)
    df.highlimit = round_series(df.highlimit * factor)

    df = df[df.ptotalcap > 0]
    df = df[df.hfqratio > 1]
    df = df.sort_values('ptotalcap')

    doProcessing(df, 1)


#prepareMediateFile()
regressionTest()
#morningTrade()

