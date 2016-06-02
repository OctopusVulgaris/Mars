# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
from collections import namedtuple
import numpy as np

import logging

max_holdings = 10
holdings = {}
summits = {}
cash = 100000.0
poolsize = 300
alert = 0.9
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'
end_date = datetime.datetime(2008,1,10)

holdings_log = pd.DataFrame(columns=('date', 'code', 'ratio_buy', 'ratio_d', 'price', 'vol', 'amount', 'cash'))
transaction_log = pd.DataFrame(columns=('date', 'type', 'code', 'price', 'volume', 'amount', 'profit', 'hfqratio', 'fee'))

MyStruct = namedtuple("MyStruct", "price volume code")

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

def allSell(code, price, date, hfqratio, type, pamo):
    if holdings.has_key(code):
        stocks = holdings[code]
        for stock in stocks:
            sell(stock, price, date, hfqratio, type, pamo)
    else:
        print 'fail to sell ' + code + ' ' + str(date)

def sell(stock, price, date, hfqratio, type, pamo):
    # profit=(price sell * hfq sell- price buy * hfq buy) * volume/ buy hfq
    global cash
    #global holdings
    #global transaction_log
    #ratio = sell ratio / buy ratio
    ratio = stock.hfqratio / hfqratio
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
    #holdings.remove(stock)


def buy(code, price, margin, date, hfqratio, pmao):
    global cash
    global summit
    #global holdings
    #global transaction_log
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

    if volume < 100:
        print 'vol less than 100 ' + str(code) + ' ' + str(date)
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

    fee = inst.amount * 0.0008
    transaction_log.loc[len(transaction_log)] = (date, 'buy', code, inst.price, volume, inst.amount, 0, hfqratio, fee)
    cash = cash - inst.amount - fee

def handle_day(x):
    #global holdings
    #global holdings_log
    date = x.index[0][0]
    #sell
    to_be_sell = []
    for code in holdings.keys():
        try:
            pos = x.index.get_loc((date, code))
            row = x.ix[pos]
            updateDayPrcRatio(code, row.hfqratio, row.open)
            # total cap out of rank 300, sell
            if not pos < 300:
                allSell(code, row.open, date, row.hfqratio, 'totalcap', row.pamo)
                to_be_sell.append(code)
                continue

            #adjsummit = newhfq/oldhfq * summit, so we save summit / oldhfq for future use
            adjSummit = summits[code] / row.hfqratio
            if adjSummit < row.phigh:
                summits[code] = row.phigh * row.hfqratio
                adjSummit = row.phigh

            # suspend for trading, continue hold
            if row.open < 0.01:
                continue
            # open high, but not reach limit, sell
            if row.open > row.phigh and row.open < row.highlimit:
                allSell(code, row.open, date, row.hfqratio, 'open high', row.pamo)
                to_be_sell.append(code)
                continue
            # open less than alert line, sell
            if row.open < adjSummit * alert:
                allSell(code, row.open, date, row.hfqratio, 'fallback', row.pamo)
                to_be_sell.append(code)
                continue
        except KeyError:
            print 'error, instrument ' + str(code) + ' not exist  on ' + str(date)

    for code in to_be_sell:
        holdings.pop(code)
        summits.pop(code)

    #buy
    if cash > 0:
        cnt = holdingNum()
        if cnt < max_holdings:
            availablCnt = max_holdings - cnt
            margin = cash / availablCnt
            valid = x.head(poolsize)
            valid = valid[valid.buyflag == True]
            for row in valid.itertuples():
                if availablCnt <= 0.01:
                    break
                code = row.Index[1]
                open = row.open
                buy(code, open, margin, date, row.hfqratio, row.pamo)
                availablCnt = availablCnt - 1

    #log
    for rows in holdings.values():
        for stock in rows:
            holdings_log.loc[len(holdings_log)] = (date, stock.code, stock.hfqratio, stock.ratio_d, stock.price, stock.volume, stock.amount, cash)



def sort(x):
    x = x.sort_values('ptotalcap', ascending=True)

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
    z = x.index
    x.reset_index(inplace=True)
    valid = x[x.open > 0.01]
    y = valid.index
    #reset index to jump halt days
    valid = valid.reset_index()

    result = pd.DataFrame()
    # yesterday
    result.loc[:, 'hfqratio'] = valid.hfqratio
    result.loc[:, 'phfqratio'] = valid.index - 1
    result.loc[:, 'phfqratio'] = result.phfqratio.map(valid.hfqratio)
    result.loc[:, 'pphfq'] = valid.index - 2
    result.loc[:, 'pphfq'] = result.pphfq.map(valid.hfqratio)
    factor = result.phfqratio / result.hfqratio
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
    factor = result.pphfq / result.hfqratio
    result.loc[:, 'plowlimit'] = valid.index - 2
    result.loc[:, 'plowlimit'] = result.plowlimit.map(valid.close)
    result.loc[:, 'plowlimit'] = result.plowlimit * factor
    result.loc[:, 'plowlimit'] = result.plowlimit * 0.9

    # on day data, value not exist for halt
    #result.loc[:, 'name'] = valid.name
    result.loc[:, 'open'] = valid.open
    result.loc[:, 'lowlimit'] = result.pclose * 0.9
    result.loc[:, 'highlimit'] = result.pclose * 1.1

    # round all price to two decimal places
    result.pclose = result.pclose.round(2)
    result.plow = result.plow.round(2)
    result.phigh = result.phigh.round(2)
    result.plowlimit = result.plowlimit.round(2)
    result.lowlimit = result.lowlimit.round(2)
    result.highlimit = result.highlimit.round(2)

    #recover to valid index first
    result = result.set_index(y)
    #recover to x.index
    result = result.reindex(x.index, fill_value=0)

    # on day data, value exist no matter haltx
    result.loc[:, 'ptotalcap'] = x.index - 1
    result.loc[:, 'ptotalcap'] = result.ptotalcap.map(x.totalcap)

    result.loc[:, 'name'] = x.name
    #result.loc[:, 'totalcap'] = x.totalcap
    result.loc[:, 'hfqratio'] = x.hfqratio
    #recover to date index
    result = result.set_index(z)
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
    df = df[df.code.str.contains(ashare_pattern)]

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
