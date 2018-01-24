# -*- coding:utf-8 -*-

import pandas as pd
import datetime as dt
import numpy as np
import ctypes as ct
import time
import talib as ta
import argparse
from utility import round_series, get_realtime_all_st, st_pattern, sendmail
import logging
import sys

from shutil import copyfile


ashare_pattern = r'^0|^3|^6'


def prepare():
    t1 = time.clock()
    day = pd.read_hdf('d:/hdf5_data/dailydata.h5', columns=['open', 'high', 'low', 'close', 'hfqratio', 'stflag'], where='date > \'2007-1-1\'')
    day = day[day.open > 0]
    day['openorg'] = day.open
    day['open'] = day.open * day.hfqratio
    day['high'] = day.high * day.hfqratio
    day['low'] = day.low * day.hfqratio
    day['close'] = day.close * day.hfqratio
    day['ocmax'] = day[['open', 'close']].max(axis=1).groupby(level=0, group_keys=False).rolling(window=67).max()
    day['ocmin'] = day[['open', 'close']].min(axis=1).groupby(level=0, group_keys=False).rolling(window=67).min()
    day['ocrate'] = day.ocmax / day.ocmin

    fd = pd.read_hdf('d:/hdf5_data/fundamental.hdf')
    day['eps'] = fd['每股收益_调整后(元)']
    day['kama'] = day.groupby(level=0).apply(
        lambda x: pd.Series(ta.KAMA(x.close.values, timeperiod=22), x.index.get_level_values(1)))
    day['kamapct'] = day.kama.groupby(level=0).pct_change()+1
    day['kamaind'] = day.kamapct.groupby(level=0, group_keys=False).rolling(window=2).max()

    a = day.groupby(level=0).last()
    a['date'] = dt.datetime.today().date()
    a = a.set_index([a.index, 'date'])
    a['open'] = 0
    day = pd.concat([day, a])

    pday = day.groupby(level=0, group_keys=False).rolling(window=2).apply(lambda x: x[0])
    day['phigh'] = pday.high
    day['popen'] = pday.open
    day['plow'] = pday.low
    day['pclose'] = pday.close
    day['pkamaind'] = pday.kamaind
    day['highlimit'] = round_series(pday.close / day.hfqratio * 1.09)
    day['lowlimit'] = round_series(pday.close / day.hfqratio * 0.906)

    day['ppocrate'] = day.ocrate.groupby(level=0, group_keys=False).rolling(window=3).apply(lambda x: x[0])
    day['ppocmax'] = day.ocmax.groupby(level=0, group_keys=False).rolling(window=3).apply(lambda x: x[0])


    day = day.reset_index()
    day = day.set_index(['date', 'code'], drop=False)
    day['date'] = day.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    day['code'] = day.code.apply(lambda x: np.int64(x))
    day = day.rename(columns={'date': 'idate', 'code': 'icode'})
    day = day.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values('ppocrate')).dropna()
    day.to_hdf('d:/hdf5_data/pttp.hdf', 'day', mode='w', format='t', complib='blosc')
    logging.info('all done...' + str(time.clock()-t1))

def initializeholding(type, prjname):
    BLSHdll = ct.cdll.LoadLibrary('D:/pttp.dll')

    BLSHdll.initialize.argtypes = [ct.c_void_p, ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_void_p,ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_void_p, ct.c_int, ct.c_double,ct.c_double, ct.c_int, ct.c_char_p]

    if type == 0:
        ll = 0
        cash = 300000
        total = 300000

        BLSHdll.initialize(ct.c_void_p(), ct.POINTER(ct.c_double)(), ct.POINTER(ct.c_double)(), ct.c_void_p(), ct.POINTER(ct.c_double)(), ct.POINTER(ct.c_double)(), ct.c_void_p(), ct.c_int(ll), ct.c_double(cash), ct.c_double(total), ct.c_int(type), ct.c_char_p(''.encode('ascii')))
        return

    initholding = pd.read_csv('d:/trade/%s/holding_pttp.csv' % (prjname), header=None, parse_dates=True, names=['date', 'code', 'buyprc','buyhfqratio', 'vol', 'daystosell', 'historyhigh', 'amount', 'cash', 'total'], dtype={'code': np.int64, 'buyprc': np.float64, 'buyhfqratio': np.float64, 'vol': np.int64, 'daystosell': np.int64, 'historyhigh': np.float64, 'amount': np.float64, 'cash': np.float64, 'total': np.float64}, index_col='date')

    if len(initholding) > 1:
        initholding = initholding.loc[initholding.index[-1]]

    ccode = initholding.code.get_values().ctypes.data_as(ct.c_void_p)
    cbuyprc = initholding.buyprc.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cbuyhfqratio = initholding.buyhfqratio.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cvol = initholding.vol.get_values().ctypes.data_as(ct.c_void_p)
    chistoryhigh = initholding.historyhigh.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    camount = initholding.amount.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cdaystosell = initholding.daystosell.get_values().ctypes.data_as(ct.c_void_p)

    ll = len(initholding)
    cash = 200000
    total = 200000
    if ll > 0:
        cash = initholding.cash.get_values()[0]
        total = initholding.total.get_values()[0]

    BLSHdll.initialize(ccode, cbuyprc, cbuyhfqratio, cvol, chistoryhigh, camount, cdaystosell, int(ll), ct.c_double(cash), ct.c_double(total), int(type), ct.c_char_p(prjname.encode('ascii')))

def doProcessing(df, params):

    dll = ct.cdll.LoadLibrary('d:/pttp.dll')

    c_double_p = ct.POINTER(ct.c_double)

    # process
    dll.process.restype = ct.c_double
    dll.process.argtypes = [ct.c_void_p, ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_int64, c_double_p]

    cdate = df.idate.get_values().ctypes.data_as(ct.c_void_p)
    ccode = df.icode.get_values().ctypes.data_as(ct.c_void_p)
    cp1 = df.eps.get_values().ctypes.data_as(c_double_p)
    cp2 = df.openorg.get_values().ctypes.data_as(c_double_p)
    cp3 = df.close.get_values().ctypes.data_as(c_double_p)
    cp4 = df.high.get_values().ctypes.data_as(c_double_p)
    cp5 = df.low.get_values().ctypes.data_as(c_double_p)
    cp6 = df.pkamaind.get_values().ctypes.data_as(c_double_p)
    cp7 = df.ppocrate.get_values().ctypes.data_as(c_double_p)
    cp8 = df.pclose.get_values().ctypes.data_as(c_double_p)
    cp9 = df.phigh.get_values().ctypes.data_as(c_double_p)
    cp10 = df.ppocmax.get_values().ctypes.data_as(c_double_p)
    cp11 = df.highlimit.get_values().ctypes.data_as(c_double_p)
    cp12 = df.lowlimit.get_values().ctypes.data_as(c_double_p)
    hfq = df.hfqratio.get_values().ctypes.data_as(c_double_p)
    cstflag = df.stflag.get_values().ctypes.data_as(ct.c_void_p)
    cactiveparam = params.get_values().ctypes.data_as(c_double_p)

    ret = dll.process(cdate, ccode, cp1, cp2, cp3, cp4, cp5, cp6, cp7, cp8, cp9, cp10, cp11, cp12, hfq, cstflag, cstflag, cstflag, len(df), cactiveparam)
    return ret

def regressionTest():
    logging.info('reading dayk tmp...' + str(dt.datetime.now()))
    df = pd.read_hdf('d:/HDF5_Data/pttp.hdf', where='date > \'2008-1-1\'')



    t1 = time.clock()
    '''
    g_maxfallback = activeparam[0]
    epsflag = activeparam[1]
    ocrateflag = activeparam[2]
    buyselladj = activeparam[3]
    g_DELAYNOSELL = int64_t(activeparam[4])
    '''
    params = pd.Series([0.92, 1.2, 1.19, 0.999, 12, 1199116800])

    for g_maxfallback in [0.92,]:
        params[0] = g_maxfallback
        for epsflag in [1.2,]:
            params[1] = epsflag
            for ocrateflag in [1.19, ]:
                params[2] = ocrateflag
                for buyselladj in [0]:
                    params[3] = buyselladj
                    for g_DELAYNOSELL in [12,]:
                        params[4] = g_DELAYNOSELL
                        for startdate in [1199116800]:
                            params[5] = startdate

                            initializeholding(0, '')
                            ret = doProcessing(df, params)
                            hfile = 'h_' + '_'.join(str(x) for x in params) + '.csv'
                            tfile = 't_' + '_'.join(str(x) for x in params) + '.csv'
                            logging.info(hfile + str(ret))
                            copyfile('d:/tradelog/transaction_pttp_c.csv', 'd:/tradelog/pttp/' + tfile)
                            copyfile('d:/tradelog/holding_pttp_c.csv', 'd:/tradelog/pttp/' + hfile)
    logging.info('doProcessing...'+str(time.clock()-t1))
    logging.info('finished...' + str(ret))

def morningTrade(prjname):
    logging.info('retrieving today all...'+ str(dt.datetime.now()))
    realtime = pd.DataFrame()
    retry = 0
    get = False
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
            time.sleep(1)

    if realtime.sort_values('date').date.iloc[-1].date() < dt.date.today():
        logging.info('today ' + str(dt.date.today()) + ' is holiday, no trading...')
        return

    logging.info('reading temp file...' + str(dt.datetime.now()))
    df = pd.read_hdf('d:/HDF5_Data/pttp.hdf', 'day', where='date = \''+str(dt.date.today()) + '\'')

    realtime = realtime[realtime.pre_close > 0]
    df = df.reset_index(0)
    df = df.reindex(realtime.index, fill_value=0)

    df.date = dt.date.today()
    df.idate = np.int64(time.mktime(dt.date.today().timetuple()))
    df.open = realtime.open
    df.low = 0.0
    df.high = 9999.0
    df.hfqratio = df.pclose / realtime.pre_close
    df.loc[realtime.name.str.contains(st_pattern), 'stflag'] = 1

    df = df[df.hfqratio > 1]
    df = df.sort_values('ppocrate')
    df.set_index([df.index, 'date']).to_hdf('d:/trade/%s/today.hdf'%prjname, 'day', format='t')

    logging.info('initializing holding...' + str(dt.datetime.now()))
    initializeholding(1, prjname)

    logging.info('doProcessing...' + str(dt.datetime.now()))
    params = pd.Series([0.92, 1.2, 1.19, 0.999, 12, 1199116800])
    doProcessing(df, params)

    logging.info('sending mail...' + str(dt.datetime.now()))
    transactions = pd.read_csv('d:/trade/%s/transaction_pttp.csv'%(prjname), header=None, parse_dates=True, names=['date', 'type', 'code', 'buyprc', 'sellprc', 'vol', 'amount', 'fee', 'cash'], index_col='date')

    try:
        transactions.type.replace({0:'buy', 9:'fallback', 8:'kama', 7:'st'}, inplace=True)
        transactions = transactions.loc[dt.date.today()]
    except KeyError:
        sendmail("no transaction today...", prjname)
    else:
        sendmail(transactions.to_string(), prjname)
    logging.info('finished %s...' % prjname)



def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str)
    parse.add_argument('-n', type=str)

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']
    prjname = args['n']

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/blsh.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    if (type == 'regression'):
        regressionTest()
    elif (type == 'prepare'):
        prepare()
    elif (type == 'trade'):
        morningTrade(prjname)


