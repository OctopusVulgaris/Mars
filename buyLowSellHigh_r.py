# -*- coding:utf-8 -*-
import socket
import sqlalchemy as sa
import pandas as pd
import datetime
import numpy as np
import tushare as ts
import argparse
import json
import numpy as np
import re
from urllib2 import urlopen, Request
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import utility
import talib
import ConfigParser

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=False)
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'

HOLDINGCSV = 'd:\\tradelog\\holding.csv'
YESTERDAYCSV = 'd:\\tradelog\\yesterday.csv'
TODAYVALIDCSV = 'd:\\tradelog\\todayValid.csv'

def GetTotalCapIndex(x):
    x = x.sort_values('totalcap')
    x = x.head(int(len(x) / 10))
    return x.totalcap.sum() / 100000

def ComputeCustomIndex():
    t1 = datetime.datetime.now()
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day')
    df = df[df.code.str.contains(ashare_pattern)]

    print datetime.datetime.now()- t1
    groupbydate = df.groupby(level=0)
    myindex = pd.DataFrame()
    myindex['trdprc'] = groupbydate.apply(GetTotalCapIndex)
    myindex['ma9'] = talib.MA(myindex.trdprc.values, timeperiod=9)
    myindex['ma12'] = talib.MA(myindex.trdprc.values, timeperiod=12)
    myindex['ma60'] = talib.MA(myindex.trdprc.values, timeperiod=60)
    myindex['ma256'] = talib.MA(myindex.trdprc.values, timeperiod=256)

    myindex.to_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day', mode='w', format='f', complib='blosc')

    print datetime.datetime.now() - t1

def round_series(s):
    s = s * 1000
    s = s.apply(round, ndigits=-1)
    return s / 1000

def sort(x):

    x = x.sort_values('totalcap', ascending=True)

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

    factor = result.phfqratio / result.hfqratio
    result['pclose'] = valid.index - 1
    result['pclose'] = result.pclose.map(valid.close)
    result['pclose'] = result.pclose * factor
    result.pclose = round_series(result.pclose)
    result['open'] = valid.open
    result['high'] = valid.high
    result['low'] = valid.low
    result['close'] = valid.close
    result['lowlimit'] = result.pclose * 0.9
    result['highlimit'] = result.pclose * 1.1
    result['tlowlimit'] = result.close * 0.9
    result['thighlimit'] = result.close * 1.1

    # round all price to two decimal places
    result.lowlimit = round_series(result.lowlimit)
    result.highlimit = round_series(result.highlimit)
    result.tlowlimit = round_series(result.tlowlimit)
    result.thighlimit = round_series(result.thighlimit)

    #recover to valid index first
    result = result.set_index(y)
    #recover to x.index
    result = result.reindex(x.index, method='pad')

    # on day data, value exist no matter haltx

    result['name'] = x.name
    result['totalcap'] = x.totalcap
    result['hfqratio'] = x.hfqratio
    #recover to date index
    result = result.set_index(z)
    return result

def prepareMediateFile(df):
    t1 = datetime.datetime.now()
    print len(df)
    df = df[df.code.str.contains(ashare_pattern)]

    df = df.sort_index()
    print datetime.datetime.now() - t1

    groupbycode = df.groupby('code')

    print 'calculating...'
    result = groupbycode.apply(calc)

    result = result[result.name.str.startswith('N') != True]
    result = result.reset_index()

    print result.columns
    result = result.set_index(['date', 'code'])
    result = result.sort_index()

    groupbydate = result.groupby(level=0)

    df = groupbydate.apply(sort)
    df = df.reset_index(level=0, drop=True)

    #df = df.loc[datetime.datetime(2008, 1, 1, ):, :]

    print datetime.datetime.now() - t1
    return df

def getHHighForCode(x):
    x.historyhigh = x.historyhigh.max()
    return x

def updateHistoryHigh(df):
    names = ('code', 'name', 'vol', 'buyprice', 'price', 'cap', 'buydate', 'historyhigh', 'cash')
    holding = pd.read_csv(HOLDINGCSV, header=0, names=names, dtype={'code': np.str, 'name': np.str}, parse_dates=True, encoding='gbk')

    # get hitory high
    df = df.sort_index()
    for i in range(0, len(holding)):
        instrument = holding.ix[i]
        hdata = df.loc(axis=0)[instrument.buydate:, instrument.code]
        if hdata.empty:
            print 'fail to find hhigh ' + (instrument.code) + ' ' + str(instrument.buydate)
            continue
        lastdayhfqratio = hdata.iloc[-1].hfqratio
        hdata['qfqratio'] = hdata.hfqratio / lastdayhfqratio
        hdata.high = hdata.high * hdata.qfqratio
        holding.loc[i, 'historyhigh'] = hdata.high.max()

    holding = holding.groupby(holding.code).apply(getHHighForCode)

    holding.to_csv(HOLDINGCSV, index=False, encoding='gbk')

def generateYesterdayFile():
    t1 = datetime.datetime.now()
    sql = "SELECT code, date, name, close, high, low, open, vol, amo, totalcap, hfqratio from dailydata where date > '2015-6-20'"
    print 'reading...'
    aa = pd.read_sql(sql, engine, index_col='date', parse_dates= True, chunksize= 100000)
    df = pd.concat(aa)

    print datetime.datetime.now() - t1

    df = prepareMediateFile(df)

    lastday = df.loc[df.reset_index(level=1).index[-1]]

    #lastday = lastday.head(300)

    lastday.to_csv(YESTERDAYCSV, encoding='gbk')

    updateHistoryHigh(df)


def trade():
    tradinglog = ''
    get = False
    todayTotal = 0;
    print 'retrieving today all...'
    today = pd.DataFrame()
    while not get:
        try:
            today = utility.get_today_all()
            if today.index.is_unique and len(today[today.open>0]) > 500:
                get = True
        except Exception:
            print 'retrying...'
    today = today.set_index('code')
    yesterday = pd.read_csv(YESTERDAYCSV, dtype={'code': np.str}, parse_dates=True, encoding='gbk')
    yesterday = yesterday.set_index('code')
    holding = pd.read_csv(HOLDINGCSV,dtype={'code': np.str}, parse_dates=True, encoding='gbk')

    print 'selling...'
    #sell
    cash = 200000
    if not holding.empty:
        cash = holding.cash[0]
    print cash
    for i in range(0, len(holding)):
        instrument = holding.ix[i]
        oneRicToday = today.loc[instrument.code]
        # trading
        if oneRicToday.open < 0.01:
            holding.loc[i, 'price'] = row.close
            continue
        else:
            holding.loc[i, 'price'] = oneRicToday.open

        pos = yesterday.index.get_loc(instrument.code)
        row = yesterday.iloc[pos]
        ratio = oneRicToday.settlement / row.close
        # 1. Check 300
        if not pos < 300:
            amount = oneRicToday.open * instrument.vol
            fee = amount * 0.0018 + instrument.vol / 1000 * 0.6
            cash = cash + amount - fee
            holding.loc[i, 'vol'] = 0
            msg =  'out 300 sell ' + instrument.code + '\n'
            tradinglog += msg
            continue

        # 2. open high, but not reach limit, sell
        phigh = row.high * ratio
        highlimit = row.thighlimit * ratio
        if oneRicToday.open > phigh and oneRicToday.open < highlimit:
            fee = amount * 0.0018 + instrument.vol / 1000 * 0.6
            cash = cash + amount - fee
            holding.loc[i, 'vol'] = 0
            msg = 'open high sell ' + instrument.code + '\n'
            tradinglog += msg
            continue
        # open less than alert line, sell
        hhigh = instrument.historyhigh * ratio
        if oneRicToday.open < round(hhigh * 1000 * 0.76, -1) / 1000:
            fee = amount * 0.0018 + instrument.vol / 1000 * 0.6
            cash = cash + amount - fee
            holding.loc[i, 'vol'] = 0
            msg = 'fallback sell ' + instrument.code + '\n'
            tradinglog += msg
            continue
    holding = holding[holding.vol > 0]
    holding.cap = holding.vol * holding.price

    print 'buying...'
    #buy
    h300 = yesterday.head(300)
    valid = pd.DataFrame()
    valid['pclose'] = h300.close
    valid['open'] = today.open
    valid['settle'] = today.settlement
    valid['ratio'] = valid.settle / valid.pclose
    valid['name'] = today.name
    valid['totalcap'] = h300.totalcap

    valid['plow'] = h300.low * valid.ratio
    valid['plowlimit'] = h300.lowlimit * valid.ratio
    valid['lowlimit'] = h300.tlowlimit * valid.ratio
    valid['highlimit'] = h300.thighlimit * valid.ratio
    valid.plow = round_series(valid.plow)
    valid.plowlimit = round_series(valid.plowlimit)
    valid.lowlimit = round_series(valid.lowlimit)
    valid.highlimit = round_series(valid.highlimit)

    valid['buyflag'] = valid.name.str.contains(st_pattern)
    valid['buyflag'] = valid.buyflag != True
    # open on this day
    valid['buyflag'] = valid.buyflag & (valid.open > 0.01)
    # open low , but not over prev low limit and don't reach today low limit
    # if halt yesterday, low == 0, this case don't buy
    valid['buyflag'] = valid.buyflag & (valid.open < valid.plow)
    valid['buyflag'] = valid.buyflag & (valid.open > valid.lowlimit)
    valid['buyflag'] = valid.buyflag & (valid.open > valid.plowlimit)

    valid.to_csv(TODAYVALIDCSV, encoding='gbk')

    print cash

    myindex = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    myindex = myindex.iloc[-1]

    availabeCash = cash
    if myindex.ma9 < myindex.ma12 and myindex.ma60 < myindex.ma256:
        availabeCash = 0
    elif myindex.ma9 < myindex.ma12 or myindex.ma60 < myindex.ma256:
        availabeCash = (holding.cap.sum() + cash) / 2 - holding.cap.sum()


    cnt = len(holding)
    if cnt < 15:
        availablCnt = 15 - cnt
        margin = cash / availablCnt
        valid = valid[valid.buyflag == True]
        valid = valid.reset_index()
        for row in valid.itertuples():
            if availablCnt <= 0.01 or availabeCash < 100:
                break
            adjMargin = margin
            if availabeCash < margin:
                adjMargin = availabeCash
            # can't buy at high limit
            if abs(row.open - row.highlimit) < 0.01:
                continue
            volume = int(adjMargin / row.open / 100) * 100

            while (adjMargin - volume*row.open - adjMargin*0.00025 - volume/1000*0.6) < 0 and volume > 0:
                volume = volume - 100
            if volume > 0:
                msg =  'buy ' + row.code + ' at price ' + str(row.open) + ' size ' + str(volume) + '\n'
                tradinglog += msg
                amount = row.open*volume
                fee = amount * 0.00025 + volume / 1000 * 0.6
                cash = cash - amount - fee
                availabeCash = availabeCash - amount - fee
                holding.loc[len(holding)] = (row.code, row.name, volume, row.open, row.open, amount, str(datetime.date.today()), row.open, 0)
                availablCnt = availablCnt - 1
    holding['cash'] = cash
    holding.to_csv(HOLDINGCSV, index=False, encoding='gbk')
    file = open('d:\\trade_log_' + str(datetime.date.today()) + '.txt', mode='w')
    file.write(tradinglog)
    file.close()

def getArgs():
    parse = argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['evening', 'morning'], default='morning', help='one of evening or morning')

    args=parse.parse_args()
    return vars(args)

def sendmail(log):
    print 'sending mail'
    config = ConfigParser.ConfigParser()
    config.read('d:\\tradelog\\mail.ini')

    fromaddr = config.get('mail', 'from') + '@' + socket.gethostname()
    toaddr = config.get('mail', 'to')
    password = config.get('mail', 'pw')
    msg = MIMEText(log, 'plain')
    msg['Subject'] = Header('BLSH@' + str(datetime.date.today()))
    msg['From'] = fromaddr
    msg['To'] = toaddr
    sm = smtplib.SMTP_SSL('smtp.qq.com')
    sm.login(fromaddr, password)
    sm.sendmail(fromaddr, toaddr.split(','), msg.as_string())
    sm.quit()

if __name__ == "__main__":
    args = getArgs()
    type = args['t']

    if (type == 'evening'):
        generateYesterdayFile()
    elif (type == 'morning'):
        log = trade()
        sendmail(log)
