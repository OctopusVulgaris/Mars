# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
from collections import namedtuple
import numpy as np

import logging

max_holdings = 10
holdings = []
cash = 100000.0
alert = 0.9
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'
end_date = datetime.datetime(2016,5,13)

holdings_log = pd.DataFrame(columns=('date', 'code', 'ratio_buy', 'ratio_d', 'price', 'vol', 'amount', 'cash'))
transaction_log = pd.DataFrame(columns=('date', 'type', 'code', 'price', 'volume', 'amount', 'profit', 'hfqratio', 'fee'))

MyStruct = namedtuple("MyStruct", "price volume code")
class instrument:
    price = 0
    volume = 0
    amount = 0
    hfqratio = 1
    ratio_d = 1
    code = None
    summit = 0

def sell(stock, price, date, hfqratio, type, pamo):
    # profit=(price sell * hfq sell- price buy * hfq buy) * volume/ buy hfq
    global cash
    #ratio = sell ratio / buy ratio
    ratio = hfqratio / stock.hfqratio
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
    #print 'sell ' + str(stock.code) + ', vol ' + str(stock.volume) + ', price ' + str(price)+' , ' + str(amount) + ' , ' + str(profit) + ', ' + str(stock.hfqratio)+ ', ' +'date ' + str(date)
    transaction_log.loc[len(transaction_log)] = (date, type, stock.code, amount/stock.volume, stock.volume, delta, profit, hfqratio, fee)
    holdings.remove(stock)


def buy(code, price, margin, date, hfqratio, pmao):
    global cash
    amo1 = 0
    amo2 = 0
    prevAmo = pmao * 0.005
    volume = 0
    tmpvol = 0
    #print 'buy ' + str(code) + ', vol ' + str(margin) + ', price ' + str(price) + ', date ' + str(date)
    if margin > pmao:
        volume = int((prevAmo / price) / 100) * 100
        tmpvol = int(((margin - prevAmo) / (price * 1.02)) / 100) * 100
        amo1 = price * volume
        amo2 = price * 1.02 * tmpvol
    else:
        volume = int((margin / price) / 100) * 100
        amo1 = price * volume
    volume = volume + tmpvol

    inst = instrument()
    inst.amount = amo1 + amo2
    inst.volume = volume
    inst.price = inst.amount / volume
    inst.code = code
    inst.hfqratio = hfqratio
    inst.ratio_d = hfqratio
    #hfq summit price
    inst.summit = price / hfqratio
    holdings.append(inst)

    fee = inst.amount * 0.0008
    transaction_log.loc[len(transaction_log)] = (date, 'buy', code, inst.price, volume, inst.amount, 0, hfqratio, fee)
    cash = cash - inst.amount - fee

def handle_day(x):
    date = x.index[0][0]
    if cash < 0:
        return
    #sell
    for stock in holdings:
        try:
            pos = x.index.get_loc((date, stock.code))
            row = x.ix[pos]
            stock.ratio_d = row.hfqratio
            stock.price = row.open
            # total cap out of rank 300, sell
            if not pos < 300:
                sell(stock, row.open, date, row.hfqratio, 'totalcap', row.pamo)
                continue

            #adjsummit = newhfq/oldhfq * summit, so we save summit / oldhfq for future use
            adjSummit = stock.summit * row.hfqratio
            if adjSummit < row.phigh:
                stock.summit = row.phigh / row.hfqratio
                adjSummit = row.phigh

            # suspend for trading, continue hold
            if row.open < 0.01:
                continue
            # open high, but not reach limit, sell
            if row.open > row.phigh and row.open < row.highlimit:
                sell(stock, row.open, date, row.hfqratio, 'open high', row.pamo)
                continue
            # open less than alert line, sell
            if row.open < adjSummit * alert:
                sell(stock, row.open, date, row.hfqratio, 'fallback', row.pamo)
                continue
        except KeyError:
            print 'error, instrument ' + str(stock.code) + ' not exist  on ' + str(date)
    #buy
    if len(holdings) < max_holdings:
        margin = cash / (max_holdings - len(holdings))
        valid = x[x.buyflag == True]
        for row in valid.itertuples():
            if len(holdings) >= max_holdings:
                break
            code = row.Index[1]
            open = row.open
            buy(code, open, margin, date, row.hfqratio, row.pamo)

    #log
    for row in holdings:
        holdings_log.loc[len(holdings_log)] = (date, row.code, row.hfqratio, row.ratio_d, row.price, row.volume, row.amount, cash)



def sort(x):
    x = x.sort_values('totalcap', ascending=True)

    # check st, final true is ready to buy
    x.loc[:, 'buyflag'] = x.name.str.contains(st_pattern)
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] != True
    # open on this day
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] & (x.open > 0.01)
    # open low , but not over prev low limit and don't reach today low limit
    # if halt yesterday, low == 0, this case don't buy
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] & (x.open < x.plow)
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] & (x.open > x.lowlimit)
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] & (x.open > x.plowlimit)
    x.loc[:, 'buyflag'] = x.loc[:, 'buyflag'] & (x.hfqratio >= 1)

    return x


def calc(x):
    x = x.sort_index()
    valid = x[x.open > 0.01]
    y = valid.index
    valid = valid.reset_index()

    result = pd.DataFrame()
    # yesterday
    result.loc[:, 'hfqratio'] = valid.hfqratio
    result.loc[:, 'phfqratio'] = valid.index - 1
    result.loc[:, 'phfqratio'] = result.phfqratio.map(valid.hfqratio)
    result.loc[:, 'pphfq'] = valid.index - 2
    result.loc[:, 'pphfq'] = result.pphfq.map(valid.hfqratio)
    factor = result.hfqratio / result.phfqratio
    result.loc[:, 'pclose'] = valid.index - 1
    result.loc[:, 'pclose'] = result.pclose.map(valid.close)
    result.loc[:, 'pclose'] = result.pclose * factor
    result.loc[:, 'plow'] = valid.index - 1
    result.loc[:, 'plow'] = result.plow.map(valid.low)
    result.loc[:, 'plow'] = result.plow * factor
    result.loc[:, 'phigh'] = valid.index - 1
    result.loc[:, 'phigh'] = result.phigh.map(valid.high)
    result.loc[:, 'phigh'] = result.phigh * factor
    result.loc[:, 'pamo'] = valid.index - 1
    result.loc[:, 'pamo'] = result.pamo.map(valid.amo)
    factor = result.phfqratio / result.pphfq
    result.loc[:, 'plowlimit'] = valid.index - 2
    result.loc[:, 'plowlimit'] = result.plowlimit.map(valid.close * factor) * 0.9

    # on day data, value not exist for halt
    #result.loc[:, 'name'] = valid.name
    result.loc[:, 'open'] = valid.open
    result.loc[:, 'lowlimit'] = result.pclose * 0.9
    result.loc[:, 'highlimit'] = result.pclose * 1.1

    result = result.set_index(y)
    result = result.reindex(x.index, fill_value=0)

    # on day data, value exist no matter halt
    result.loc[:, 'name'] = x.name
    result.loc[:, 'totalcap'] = x.totalcap
    result.loc[:, 'hfqratio'] = x.hfqratio
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

def prepareMediateFile():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf','day')

    print len(df)

    df.sort_index(inplace=True)
    print datetime.datetime.now() - t1

    groupbycode = df.groupby('code')

    print 'calculating...'
    result = groupbycode.apply(calc)
    result = result.reset_index()
    print result.columns
    result = result.set_index(['date', 'code'])
    result = result.sort_index()

    groupbydate = result.groupby(level=0)
    df = groupbydate.apply(sort)
    df = df.reset_index(level=0, drop=True)

    df = df.loc[datetime.datetime(2008, 1, 1, ):, :]

    print datetime.datetime.now() - t1
    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf','day',mode='w', format='t', complib='blosc')

    print len(df)
    print datetime.datetime.now() - t1

def Processing():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day')
    df = df.loc[datetime.datetime(2008, 1, 7, ):, :]
    print datetime.datetime.now()- t1
    groupbydate = df.groupby(level=0)
    groupbydate.apply(handle_day)
    print datetime.datetime.now() - t1
    print 'cash: ' + str(cash)
    transaction_log.to_csv('d:\\transaction_log.csv')
    global holdings_log
    holdings_log.vol = holdings_log.vol * holdings_log.ratio_d
    holdings_log.vol = holdings_log.vol / holdings_log.ratio_buy
    holdings_log.amount = holdings_log.price * holdings_log.vol
    aa = holdings_log.groupby('date')['amount'].sum()
    holdings_log = holdings_log.set_index('date')
    aa.reindex(holdings_log.index, method='bfill')
    holdings_log['total'] = holdings_log.cash + aa
    holdings_log.to_csv('d:\\holdings_log.csv')

#csvtoHDF()
#prepareMediateFile()
Processing()
