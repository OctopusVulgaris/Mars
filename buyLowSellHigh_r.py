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
from utility import round_series, get_today_all
import talib
import ConfigParser

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=False)
st_pattern = r'^S|^*|退市'


HOLDINGCSV = 'd:\\tradelog\\holding.csv'
YESTERDAYCSV = 'd:\\tradelog\\yesterday.csv'
TODAYVALIDCSV = 'd:\\tradelog\\todayValid.csv'

def GetTotalCapIndex(x):
    x = x.sort_values('totalcap')
    x = x.head(int(len(x) / 10))
    return x.totalcap.sum() / 100000

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

def sort(x):

    x = x.sort_values('totalcap', ascending=True)

    return x


def calc(x):

    #x = x.sort_index()
    #z = x.index
    #x.reset_index(inplace=True)
    valid = x[x.open > 0.01]
    #y = valid.index
    #reset index to jump halt days
    #valid = valid.reset_index()

    #result = pd.DataFrame()
    validLen = len(valid)
    # yesterday
    #result['hfqratio'] = valid.hfqratio
    valid['phfqratio'] = 1
    valid['phfqratio'].iloc[1:] = valid.hfqratio.values[:validLen-1]
    #result['phfqratio'] = valid.index - 1
    #result['phfqratio'] = result.phfqratio.map(valid.hfqratio)

    factor = valid.phfqratio / valid.hfqratio
    valid['pclose'] = 0
    valid['pclose'].iloc[1:] = valid.close.values[:validLen-1]
    #result['pclose'] = valid.index - 1
    #result['pclose'] = result.pclose.map(valid.close)
    valid['pclose'] = valid.pclose * factor
    valid.pclose = round_series(valid.pclose)
    #result['open'] = valid.open
    #result['high'] = valid.high
    #result['low'] = valid.low
    #result['close'] = valid.close
    valid['lowlimit'] = valid.pclose * 0.9
    valid['highlimit'] = valid.pclose * 1.1
    valid['tlowlimit'] = valid.close * 0.9
    valid['thighlimit'] = valid.close * 1.1

    # round all price to two decimal places
    valid.lowlimit = round_series(valid.lowlimit)
    valid.highlimit = round_series(valid.highlimit)
    valid.tlowlimit = round_series(valid.tlowlimit)
    valid.thighlimit = round_series(valid.thighlimit)

    #recover to valid index first
    #result = result.set_index(y)
    #recover to x.index
    valid = valid.reindex(x.index, method='pad')

    # on day data, value exist no matter haltx

    #valid['name'] = x.name
    valid['totalcap'] = x.totalcap
    valid['hfqratio'] = x.hfqratio
    #recover to date index
    #result = result.set_index(z)
    return valid

def prepareMediateFile(df):
    t1 = datetime.datetime.now()
    print len(df)

    #df = df.sort_index()
    #print datetime.datetime.now() - t1

    groupbycode = df.groupby(level=0)

    print 'calculating...'
    result = groupbycode.apply(calc)

    result = result[result.name.str.startswith('N') != True]
    result = result.swaplevel(i='code', j='date')
    #result = result.reset_index()

    #print result.columns
    #result = result.set_index(['date', 'code'])
    #result = result.sort_index()


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

    if holding.empty:
        print 'empty holding.'
        return
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
    #sql = "SELECT code, date, name, close, high, low, open, vol, amo, totalcap, hfqratio from dailydata where date > '2015-1-1'"
    print 'reading...'
    #aa = pd.read_sql(sql, engine, index_col='date', parse_dates= True, chunksize= 100000)
    #df = pd.concat(aa)
    df = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['close', 'high', 'low', 'open', 'totalcap', 'name', 'hfqratio'], where='date > \'2015-1-1\'')

    print datetime.datetime.now() - t1

    df = prepareMediateFile(df)

    lastday = df.loc[df.reset_index(level=1).index[-1]]

    #lastday = lastday.head(300)

    lastday.to_csv(YESTERDAYCSV, encoding='gbk')

    updateHistoryHigh(df)

    ComputeCustomIndex(df)

def trade():
    tradinglog = ''

    get = False
    todayTotal = 0;
    print 'retrieving today all...'
    today = pd.DataFrame()
    retry = 0
    while not get and retry < 15:
        try:
            retry += 1
            today = get_today_all()
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
        amount = oneRicToday.open * instrument.vol
        # 1. Check 300
        if not pos < 300:
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

    valid.to_csv(TODAYVALIDCSV, encoding='gbk')
    holding['cash'] = cash
    holding.to_csv(HOLDINGCSV, index=False, encoding='gbk')

    file = open('d:\\tradelog\\trade_log_' + str(datetime.date.today()) + '.txt', mode='w')
    file.write(tradinglog)
    file.close()

    return tradinglog

def getArgs():
    parse = argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['evening', 'morning'], default='morning', help='one of evening or morning')

    args=parse.parse_args()
    return vars(args)

def sendmail(log):
    print 'sending mail'
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
        print str(e)


if __name__ == "__main__":
    args = getArgs()
    type = args['t']

    if (type == 'evening'):
        generateYesterdayFile()
    elif (type == 'morning'):
        log = trade()
        sendmail(log)
