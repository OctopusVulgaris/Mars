# -*- coding:utf-8 -*-

import pandas as pd
import sqlalchemy as sa
import datetime
from collections import namedtuple
import numpy as np
import ctypes as ct
import time
import talib


import logging
import talib as ta


max_holdings = 10
holding_cnt = 0
holdings = {}
summits = {}
to_be_sell = []
cash = 100000.0
poolsize = 300
alert = 900
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'

end_date = datetime.datetime(2008,1,10)
tax_date = datetime.datetime(2008,9,19)

holdings_log = []
transaction_log = []
#holdings_log = pd.DataFrame(columns=('date', 'code', 'ratio_buy', 'ratio_d', 'price', 'vol', 'amount', 'cash'))
#transaction_log = pd.DataFrame(columns=('date', 'type', 'code', 'price', 'volume', 'amount', 'profit', 'hfqratio',
# 'fee'))

def GetTotalCapIndex(x):
    x = x.sort_values('ptotalcap')
    x = x.head(int(len(x) / 10))
    return x.ptotalcap.sum() / 100000

def holdingNum():
    cnt =0
    for code in holdings.keys():
        cnt += len(holdings[code])
    return cnt

class instrument:
    price = 0
    volume = 0
    amount = 0
    hfqratio = 1
    ratio_d = 1
    code = None
    #summit = 0

def updateDayPrcRatio(code, hfqratio, open):
    stocks = holdings[code]
    for stock in stocks:
        stock.ratio_d = hfqratio
        stock.price = open

def sell(stock, price, date, hfqratio, type, pamo):
    global cash
    #global holding_cnt
    ratio = hfqratio /stock.hfqratio
    stock.volume = stock.volume * ratio
    prevAmo = pamo * 0.005
    amount = price * stock.volume
    if amount > prevAmo:
        vol1 = int(prevAmo / price / 100) * 100
        vol2 = stock.volume - vol1
        amount = vol1 * price + vol2 * price *0.98

    fee = amount * 0.0018 + stock.volume / 1000 * 0.6
    profit = amount - stock.amount - fee
    delta = amount - fee
    cash = cash + delta
    print 'sell ' + str(stock.code) + ', vol ' + str(stock.volume) + ', price ' + str(price)+' , ' + str(amount) + ' , ' + str(profit) + ', ' + str(stock.hfqratio)+ ', ' +'date ' + str(date)
    #holding_cnt -= 1
    transaction_log.append((date, type, stock.code, amount/stock.volume, stock.volume, amount,  profit, hfqratio, fee))


def allSell(code, date, row, type):
    if (row.high - row.low) < 0.01 and (row.high - row.lowlimit) < 0.01:
        #can't sell on low limit
        return
    if holdings.has_key(code):
        to_be_sell.append(code)
        stocks = holdings[code]
        for stock in stocks:
            sell(stock, row.open, date, row.hfqratio, type, row.pamo)
    else:
        print 'fail to sell ' + code + ' ' + str(date)


def buy(code, price, margin, date, hfqratio, pmao):
    global cash
    global summit
    #global holding_cnt
    factor = 1.0
    if date < tax_date:
        factor = 0.0018
    else:
        factor = 0.0008
    margin = margin * (1 - factor)
    amo1 = 0
    amo2 = 0
    prevAmo = pmao * 0.005
    volume = 0
    tmpvol = 0
    print 'buy ' + str(code) + ', vol ' + str(margin) + ', price ' + str(price) + ', date ' + str(date)
    if margin > pmao:
        volume = int(prevAmo / price / 100) * 100
        tmpvol = int((margin - prevAmo) / (price * 1.02) / 100) * 100
        amo1 = price * volume
        amo2 = price * 1.02 * tmpvol
    else:
        volume = int(margin / price / 100) * 100
        amo1 = price * volume
    volume = volume + tmpvol

    if volume < 100:
        #print 'vol less than 100 ' + str(code) + ' ' + str(date)
        return
    inst = instrument()
    inst.amount = amo1 + amo2
    inst.volume = volume
    inst.price = inst.amount / volume
    inst.code = code
    inst.hfqratio = hfqratio
    inst.ratio_d = hfqratio
    #hfq summit price
    hfqPrc = price * hfqratio
    if summits.has_key(code):
        summits[code] = max(summits[code], hfqPrc)
    else:
        summits[code] = hfqPrc

    if holdings.has_key(code):
        holdings[code].append(inst)
    else:
        holdings[code] = [inst]

    fee = inst.amount * factor + volume / 1000 * 0.6
    #transaction_log.loc[len(transaction_log)] = (date, 'buy', code, inst.price, volume, inst.amount, 0, hfqratio, fee)
    transaction_log.append((date, 'buy', code, inst.price, volume, inst.amount, 0, hfqratio, fee))
    cash = cash - inst.amount - fee
    #holding_cnt += 1


def handle_day(x):
    #global holdings
    #global holdings_log
    date = x.index[0][0]
    #sell

    for code in holdings.keys():
        try:
            pos = x.index.get_loc((date, code))
            row = x.iloc[pos]
            # suspend for trading, continue hold
            if row.open < 0.01:
                continue

            updateDayPrcRatio(code, row.hfqratio, row.open)
            # total cap out of rank 300, sell
            if not pos < 300:
                allSell(code, date, row, 'totalcap')
                continue

            #adjsummit = newhfq/oldhfq * summit, so we save summit / oldhfq for future use
            adjSummit = summits[code] / row.hfqratio
            if adjSummit < row.phigh:
                summits[code] = row.phigh * row.hfqratio
                adjSummit = row.phigh

            # open high, but not reach limit, sell
            if row.open > row.phigh and row.open < row.highlimit:
                allSell(code, date, row, 'open high')
                continue
            # open less than alert line, sell
            if row.open < round(adjSummit * alert, -1) / 1000:
                allSell(code, date, row, 'fallback')
                continue
        except KeyError:
            print 'error, instrument ' + str(code) + ' not exist  on ' + str(date)

    global to_be_sell
    for code in to_be_sell:
        holdings.pop(code)
        summits.pop(code)
    to_be_sell[:] = []


    #buy
    if cash > 0:
        cnt = holdingNum()
        #print 'cnt=' + str(cnt)
        if cnt < max_holdings:
            availablCnt = max_holdings - cnt
            margin = cash / availablCnt
            valid = x.head(poolsize)
            valid = valid[valid.buyflag == True]
           # print 'valid len=' + str(len(valid))
            for row in valid.itertuples():
                if availablCnt <= 0.01:
                    #print 'avaCnt=' + str(availablCnt)
                    break
                code = row.Index[1]
                open = row.open
                #can't buy at high limit
                if abs(row.open - row.highlimit) < 0.01:
                    #print 'open==highlimit' + str(open)
                    continue
                buy(code, open, margin, date, row.hfqratio, row.pamo)
                availablCnt = availablCnt - 1

    #log
    for rows in holdings.values():
        for stock in rows:
            #holdings_log.loc[len(holdings_log)] = (date, stock.code, stock.hfqratio, stock.ratio_d, stock.price,
    # stock.volume, stock.amount, cash)
            holdings_log.append((date, stock.code, stock.hfqratio, stock.ratio_d, stock.price, stock.volume, stock.amount, cash))



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

    x = x.sort_index()
    z = x.index
    x.reset_index(inplace=True)
    valid = x[x.open > 0.01]
    y = valid.index
    #reset index to jump halt days
    valid = valid.reset_index()

    result = pd.DataFrame()
    # yesterday
    result['hfqratio'] = valid.hfqratio
    result['phfqratio'] = valid.index - 1
    result['phfqratio'] = result.phfqratio.map(valid.hfqratio)
    result['pphfq'] = valid.index - 2
    result['pphfq'] = result.pphfq.map(valid.hfqratio)
    factor = result.phfqratio / result.hfqratio
    result['pclose'] = valid.index - 1
    result['pclose'] = result.pclose.map(valid.close)
    result['pclose'] = result.pclose * factor
    result['plow'] = valid.index - 1
    result['plow'] = result.plow.map(valid.low)
    result['plow'] = result.plow * factor
    result['phigh'] = valid.index - 1
    result['phigh'] = result.phigh.map(valid.high)
    result['phigh'] = result.phigh * factor
    result['pamo'] = valid.index - 1
    result['pamo'] = result.pamo.map(valid.amo)
    factor = result.pphfq / result.hfqratio
    result['plowlimit'] = valid.index - 2
    result['plowlimit'] = result.plowlimit.map(valid.close)
    result['plowlimit'] = result.plowlimit * factor
    result['plowlimit'] = result.plowlimit * 900

    # on day data, value not exist for halt
    #result.loc[:, 'name'] = valid.name
    result.pclose = result.pclose.apply(round, ndigits=2)
    result['open'] = valid.open
    result['high'] = valid.high
    result['low'] = valid.low
    result['lowlimit'] = result.pclose * 900
    result['highlimit'] = result.pclose * 1100

    # round all price to two decimal places
    result.plow = result.plow.apply(round, ndigits=2)
    result.phigh = result.phigh.apply(round, ndigits=2)
    result.lowlimit = result.lowlimit.apply(round, ndigits=-1)
    result.highlimit = result.highlimit.apply(round, ndigits=-1)
    result.plowlimit = result.plowlimit.apply(round, ndigits=-1)
    result.lowlimit= result.lowlimit / 1000
    result.highlimit = result.highlimit / 1000
    result.plowlimit = result.plowlimit / 1000

    #recover to valid index first
    result = result.set_index(y)
    #recover to x.index
    result = result.reindex(x.index, fill_value=0)

    # on day data, value exist no matter haltx
    result['ptotalcap'] = x.index - 1
    result['ptotalcap'] = result.ptotalcap.map(x.totalcap)
    #result['ptradeablecap'] = x.index - 1
    #result['ptradeablecap'] = result.ptradeablecap.map(x.tradeablecap)

    result['name'] = x.name
    #result.loc[:, 'totalcap'] = x.totalcap
    result['hfqratio'] = x.hfqratio
    #recover to date index
    result = result.set_index(z)
    del result['phfqratio']
    del result['pphfq']
    return result


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

def prepareMediateFile():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5','dayk', where='date > \'2006-5-1\'')
    #df = df[df.code.str.contains(ashare_pattern)]


    print len(df)

    df.sort_index(inplace=True)
    print datetime.datetime.now() - t1

    groupbycode = df.groupby(level=0)

    print 'calculating...'
    df = groupbycode.apply(calc)

    df = df[df.name.str.startswith('N') != True]
    df = df.reset_index()

    print df.columns
    df = df.set_index(['date', 'code'], drop=False)
    df.date = df.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    df.code = df.code.apply(lambda x: np.int64(x))
    df = df.rename(columns={'date': 'idate', 'code': 'icode'})
    df = df.sort_index()

    groupbydate = df.groupby(level=0)

    df = groupbydate.apply(sort)
    df = df.reset_index(level=0, drop=True)

    ComputeCustomIndex(df)
    print datetime.datetime.now() - t1
    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf','day',mode='w', format='t', complib='blosc')

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

    myindex.to_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day', mode='w', format='f', complib='blosc')

    #print datetime.datetime.now() - t1

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
    setloglevel(1)
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

    #groupbydate = df.groupby(level=0)
    #groupbydate.apply(handle_day)

    #print 'cash: ' + str(cash)
    #t_log = pd.DataFrame(transaction_log)
    #t_log.columns=('date', 'type', 'code', 'price', 'volume', 'amount', 'profit', 'hfqratio', 'fee')
    #t_log.to_csv('d:\\transaction_log.csv')
    #h_log = pd.DataFrame(holdings_log)
    #h_log.columns = ('date', 'code', 'ratio_buy', 'ratio_d', 'price', 'vol', 'amount', 'cash')
    #h_log.vol = h_log.vol * h_log.ratio_d
    #h_log.vol = h_log.vol / h_log.ratio_buy
    #h_log.amount = h_log.price * h_log.vol
    #aa = h_log.groupby('date')['amount'].sum()
    #h_log = h_log.set_index('date')
    #aa.reindex(h_log.index, method='bfill')
    #h_log['total'] = h_log.cash + aa
    #h_log.to_csv('d:\\holdings_log.csv')
    print time.clock()


#sqltoHDF()
#prepareMediateFile()
Processing()

