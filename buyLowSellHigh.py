# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
from collections import namedtuple

import logging

max_holdings = 10
holdings = []
cash = 100000.0
alert = 0.9
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'
end_date = datetime.datetime(2016,5,13)

holdings_log = pd.DataFrame(columns=('date', 'code', 'amount', 'cash'))
transaction_log = pd.DataFrame(columns=('date', 'type', 'code', 'price', 'volume', 'amount', 'profit', 'hfqratio'))

MyStruct = namedtuple("MyStruct", "price volume code")
class instrument:
    price = 0
    volume = 0
    amount = 0
    hfqratio = 1
    code = None
    summit = 0

def sell(stock, price, date, hfqratio, type):
    # profit=(price sell * hfq sell- price buy * hfq buy) * volume/ buy hfq
    global cash
    profit = (price * hfqratio - stock.price * stock.hfqratio) * stock.volume / stock.hfqratio
    delta = stock.amount + profit
    cash = cash + delta
    print 'sell ' + str(stock.code) + ', vol ' + str(stock.volume) + ', price ' + str(price)+' , ' \
        '' + str(stock.amount) + ' , ' + str(profit) + ', ' + str(stock.hfqratio)+ ', ' +'date ' + str(date)
    transaction_log.loc[len(transaction_log)] = (date, type, stock.code, price, stock.volume, delta, profit, hfqratio)
    holdings.remove(stock)


def buy(code, price, margin, date, hfqratio):
    global cash
    print 'buy ' + str(code) + ', vol ' + str(margin) + ', price ' + str(price) + ', date ' + str(date)
    volume = int((margin / price) / 100) * 100
    inst = instrument()
    inst.price = price
    inst.volume = volume
    inst.code = code
    inst.amount = price * volume
    inst.hfqratio = hfqratio
    #hfq summit price
    inst.summit = price * hfqratio
    holdings.append(inst)

    transaction_log.loc[len(transaction_log)] = (date, 'buy', code, price, volume, inst.amount, 0, hfqratio)
    cash = cash - inst.amount

def handle_day(x):
    date = x.index[0][0]
    if date > end_date or cash < 0:
        return
    #sell
    for stock in holdings:
        try:
            pos = x.index.get_loc((date, stock.code))
            row = x.ix[pos]
            if (date == end_date):
                sell(stock, row.open, date, row.hfqratio, 'end date')
                continue
            # total cap out of rank 300, sell
            if not pos < 300:
                sell(stock, row.open, date, row.hfqratio, 'totalcap')
                continue

            if stock.summit < row.phigh:
                stock.summit = row.phigh

            # suspend for trading, continue hold
            if row.open < 0.01:
                continue
            # open high, but not reach limit, sell
            if row.open > row.phigh and row.open < row.highlimit:
                sell(stock, row.open, date, row.hfqratio, 'open high')
                continue
            # open less than alert line, sell
            if row.open < stock.summit * alert:
                sell(stock, row.open, date, row.hfqratio, 'fallback')
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
            buy(code, open, margin, date, row.hfqratio)

    #log
    for row in holdings:
        holdings_log.loc[len(holdings_log)] = (date, row.code, row.amount, cash)



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

    return x


def calc(x):
    x = x.sort_index()
    valid = x[x.open > 0.01]
    y = valid.index
    valid = valid.reset_index()

    result = pd.DataFrame()
    # yesterday
    result.loc[:, 'pclose'] = valid.index - 1
    result.loc[:, 'pclose'] = result.pclose.map(valid.close)
    result.loc[:, 'plow'] = valid.index - 1
    result.loc[:, 'plow'] = result.plow.map(valid.low)
    result.loc[:, 'phigh'] = valid.index - 1
    result.loc[:, 'phigh'] = result.phigh.map(valid.high)
    result.loc[:, 'pamo'] = valid.index - 1
    result.loc[:, 'pamo'] = result.pamo.map(valid.amo)
    result.loc[:, 'plowlimit'] = valid.index - 2
    result.loc[:, 'plowlimit'] = result.plowlimit.map(valid.close) * 0.9

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

def updateTmpHDF():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf','day')

    print len(df)

    df.sort_index(inplace=True)
    df = df.loc[datetime.datetime(2008, 1, 1, ):, :]
    print datetime.datetime.now() - t1

    #remove st
    #pattern = r'^ST|^S|^\*ST'
    #df.loc[:,'flag'] = df.name.str.contains(pattern)
    #df = df[df.flag==False]
    #del df['flag']
    #print len(df)

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




    print datetime.datetime.now() - t1
    print 'saving...'
    df.to_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf','day',mode='w', format='t', complib='blosc')

    print len(df)
    print datetime.datetime.now() - t1

def ProcessTmpHDF():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day')
    print datetime.datetime.now()- t1
    groupbydate = df.groupby(level=0)
    groupbydate.apply(handle_day)
    print datetime.datetime.now() - t1
    print 'cash: ' + str(cash)
    transaction_log.to_csv('d:\\transaction_log.csv')
    holdings_log.to_csv('d:\\holdings_log.csv')



#updateTmpHDF()
ProcessTmpHDF()
