# -*- coding:utf-8 -*-

import pandas as pd
import datetime
import numpy as np
import ctypes as ct
import time
import talib
import argparse
from utility import round_series, get_realtime_all_st
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import socket
import ConfigParser
import logging
import sys


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
    headlen = int(len(x) / 10)
    x = x.head(headlen)
    prc = x.ptotalcap.sum() / headlen
    return prc / 100000


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


def ComputeCustomIndex(df):
    #t1 = datetime.datetime.now()
    #df = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day')
    #df = df[df.code.str.contains(ashare_pattern)]

    groupbydate = df.groupby(level=0)
    myindex = pd.DataFrame()
    myindex['trdprc'] = groupbydate.apply(GetTotalCapIndex)
    myindex['ma9'] = talib.MA(myindex.trdprc.values, timeperiod=9)
    myindex['ma12'] = talib.MA(myindex.trdprc.values, timeperiod=12)
    myindex['ma60'] = talib.MA(myindex.trdprc.values, timeperiod=60)
    myindex['ma256'] = talib.MA(myindex.trdprc.values, timeperiod=256)
    myindex = myindex.fillna(0)

    myindex.to_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day', mode='w', format='f', complib='blosc')


def prepareMediateFile():
    logging.info('reading dailydata.h5...' + str(datetime.datetime.now()))
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5','dayk', columns=['close', 'high', 'low', 'open', 'totalcap', 'tradeablecap', 'name', 'hfqratio'], where='date > \'2006-5-1\'')
    #df = df[df.code.str.contains(ashare_pattern)]
    logging.info('sorting, [code, date]...' + str(datetime.datetime.now()))

    df = df[df.tradeablecap > 0]
    df.sort_index(inplace=True)

    logging.info('calculating...' + str(datetime.datetime.now()))
    groupbycode = df.groupby(level=0)
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

    logging.info('switching index...' + str(datetime.datetime.now()))
    df = df[df.ptotalcap > 0]

    df = df.reset_index()
    df = df.set_index(['date', 'code'], drop=False)
    df.date = df.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    df.code = df.code.apply(lambda x: np.int64(x))
    df = df.rename(columns={'date': 'idate', 'code': 'icode'})

    logging.info('sorting, [date, code]...' + str(datetime.datetime.now()))
    df = df.sort_index()
    groupbydate = df.groupby(level=0)
    df = groupbydate.apply(sort)
    df = df.reset_index(level=0, drop=True)

    logging.info('compouting cumstom index...' + str(datetime.datetime.now()))
    ComputeCustomIndex(df)

    logging.info('saving...' + str(datetime.datetime.now()))
    df.to_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf','day',mode='w', format='t', complib='blosc')

    logging.info('all done...' + str(datetime.datetime.now()))

def initializeholding():

    initholding = pd.read_csv('d:\\tradelog\\holding_real_c.csv', header=None, parse_dates=True, names=['date', 'code', 'buyprc', 'buyhfqratio', 'vol', 'historyhigh', 'amount', 'cash', 'total'], dtype={'code': np.int64, 'buyprc': np.float64, 'buyhfqratio': np.float64, 'vol': np.int64, 'historyhigh': np.float64, 'amount': np.float64, 'cash': np.float64, 'total': np.float64}, index_col='date')

    if initholding.empty:
        return

    initholding = initholding.loc[initholding.index[-1]]

    BLSHdll = ct.cdll.LoadLibrary('d:\\BLSH.dll')

    BLSHdll.setindex.argtypes = [ct.c_void_p, ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_void_p, ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_int, ct.c_double, ct.c_double]

    ccode = initholding.code.get_values().ctypes.data_as(ct.c_void_p)
    cbuyprc = initholding.buyprc.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cbuyhfqratio = initholding.buyhfqratio.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cvol = initholding.vol.get_values().ctypes.data_as(ct.c_void_p)
    chistoryhigh = initholding.historyhigh.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    camount = initholding.amount.get_values().ctypes.data_as(ct.POINTER(ct.c_double))

    if initholding.empty:
        BLSHdll.initialize(ccode, cbuyprc, cbuyhfqratio, cvol, chistoryhigh, camount, len(initholding), ct.c_double(100000.0), ct.c_double(100000.0))
    else:
        BLSHdll.initialize(ccode, cbuyprc, cbuyhfqratio, cvol, chistoryhigh, camount, len(initholding), ct.c_double(initholding.cash.get_values()[0]), ct.c_double(initholding.total.get_values()[0]))

def doProcessing(df, index, loglevel):

    BLSHdll = ct.cdll.LoadLibrary('d:\\BLSH.dll')

    c_double_p = ct.POINTER(ct.c_double)
    # set log level
    BLSHdll.setloglevel.argtypes = [ct.c_int64]
    BLSHdll.setloglevel(loglevel)
    # set index
    BLSHdll.setindex.restype = ct.c_int64
    BLSHdll.setindex.argtypes = [ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_int]

    cdate = index.index.to_series().apply(lambda x: np.int64(time.mktime(x.timetuple()))).get_values().ctypes.data_as(ct.c_void_p)
    cprc = index.trdprc.get_values().ctypes.data_as(c_double_p)
    cma1 = index.ma9.get_values().ctypes.data_as(c_double_p)
    cma2 = index.ma12.get_values().ctypes.data_as(c_double_p)
    cma3 = index.ma60.get_values().ctypes.data_as(c_double_p)
    cma4 = index.ma256.get_values().ctypes.data_as(c_double_p)

    BLSHdll.setindex(cdate, cprc, cma1, cma2, cma3, cma4, len(index))

    # process
    BLSHdll.process.restype = ct.c_int64
    BLSHdll.process.argtypes = [ct.c_void_p, ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_int]


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
    cupperamo = df.upperamo.get_values().ctypes.data_as(ct.c_void_p)
    cloweramo = df.loweramo.get_values().ctypes.data_as(ct.c_void_p)

    ret = BLSHdll.process(cdate, ccode, cpclose, cphigh, cplow, cplowlimit, copen, chighlimit, clowlimit, chfqratio, cstflag, cupperamo, cloweramo, len(df))

        # ti = ct.cdll.LoadLibrary('d:\\BLSH.dll').testint
        # td = ct.cdll.LoadLibrary('d:\\BLSH.dll').testdouble
        # tui = ct.cdll.LoadLibrary('d:\\BLSH.dll').testuint
        # ti.argtypes = [ct.c_void_p, ct.c_int]
        # td.argtypes = [c_double_p, ct.c_int]
        # tui.argtypes = [ct.c_void_p, ct.c_int]


def regressionTest():
    logging.info('reading dayk tmp...' + str(datetime.datetime.now()))
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', where='date > \'2008-1-6\'')

    logging.info('reading open split amount data...' + str(datetime.datetime.now()))
    osa = pd.read_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', where='date > \'2008-1-6\'')
    osa = osa.swaplevel(i='code', j='date')
    osa = osa.reindex(df.index, fill_value=0)

    df['upperamo'] = osa.upperamo
    df['loweramo'] = osa.loweramo

    index = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    index = index.fillna(0)
    index = index.loc['2008-1-1':]

    logging.info('doProcessing...' + str(datetime.datetime.now()))
    doProcessing(df, index, 1)
    logging.info('finished...' + str(datetime.datetime.now()))

def morningTrade():
    logging.info('retrieving today all...'+ str(datetime.datetime.now()))
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

    if (realtime.date.iloc[-1].date() < datetime.date.today()) & (realtime.date.iloc[0].date() < datetime.date.today()):
        logging.info('today ' + str(datetime.date.today()) + ' is holiday, no trading...')
        return

    logging.info('reading temp file...' + str(datetime.datetime.now()))
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', where='date = \'2050-1-1\'')

    realtime = realtime[realtime.pre_close > 0]
    df = df.reset_index(0)
    df.reindex(realtime.index, fill_value=0)

    df.date = datetime.date.today()
    df.idate = np.int64(time.mktime(datetime.date.today().timetuple()))
    df.open = realtime.open
    df.hfqratio = df.phfqratio * df.pclose / realtime.pre_close
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
    df['upperamo'] = np.int64(0)
    df['loweramo'] = np.int64(0)

    df.to_hdf('d:/today.hdf', 'day')

    index = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    index = index.fillna(0)
    index = index.loc['2008-1-1':]
    index.loc[datetime.date.today()] = index.loc['2050-1-1']

    logging.info('initializing holding...' + str(datetime.datetime.now()))
    initializeholding()

    logging.info('doProcessing...' + str(datetime.datetime.now()))
    doProcessing(df, index, 1)

    logging.info('sending mail...' + str(datetime.datetime.now()))
    transactions = pd.read_csv('d:\\tradelog\\transaction_real_c.csv', header=None, parse_dates=True, names=['date', 'type', 'code', 'prc', 'vol', 'amount', 'fee', 'cash'], index_col='date')
    transactions.date = datetime.date.today()
    sendmail(transactions.to_string())
    logging.info('finished...' + str(datetime.datetime.now()))

def sendmail(log):
    config = ConfigParser.ConfigParser()
    config.read('d:\\tradelog\\mail.ini')

    fromaddr = config.get('mail', 'from')
    toaddr = config.get('mail', 'to')
    password = config.get('mail', 'pw')
    msg = MIMEText(log, 'plain')
    msg['Subject'] = Header('BLSH@' + str(datetime.date.today())  + '_' + socket.gethostname())
    msg['From'] = fromaddr
    msg['To'] = toaddr

    try:
        sm = smtplib.SMTP_SSL('smtp.qq.com')
        sm.ehlo()
        sm.login(fromaddr, password)
        sm.sendmail(fromaddr, toaddr.split(','), msg.as_string())
        sm.quit()
    except Exception, e:
        logging.error(str(e))

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['prepare', 'regression', 'trade'], default='regression', help='one of \'prepare\', \'regression\', \'trade\'')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']

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
        prepareMediateFile()
    elif (type == 'trade'):
        morningTrade()


