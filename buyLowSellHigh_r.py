# -*- coding:utf-8 -*-

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

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=False)
st_pattern = r'^ST|^S|^\*ST|退市'
ashare_pattern = r'^0|^3|^6'


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

def updateHoldingFile(df):
    names = ('code', 'name', 'vol', 'validvol', 'freezevol', 'buyprice', 'earnprice', 'price', 'pct', 'net', 'cap', 'market', 'account', 'ontheway', 'buydate', 'historyhigh', 'cash')
    holding = pd.read_csv('d:\\holding.csv', header=0, names=names, dtype={'code': np.str, 'name': np.str}, parse_dates=True)

    # get hitory high
    df = df.sort_index()
    for i in range(0, len(holding)):
        instrument = holding.ix[i]
        hdata = df.loc(axis=0)[instrument.buydate:, instrument.code]
        lastdayhfqratio = hdata.iloc[-1].hfqratio
        hdata['qfqratio'] = hdata.hfqratio / lastdayhfqratio
        hdata.high = hdata.high * hdata.qfqratio
        holding.historyhigh[i] = hdata.high.max()

    holding.to_csv('d:\\holding.csv', index=False)

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

    lastday.to_csv('d:\\yesterday.csv', encoding='gbk')

    updateHoldingFile(df)


def trade():
    get = False
    today = pd.DataFrame()
    while not get:
        try:
            today = ts.get_today_all()
            if today.index.is_unique:
                get = True
        except Exception:
            print 'retrying...'
    today = today.set_index('code')
    yesterday = pd.read_csv('d:\\yesterday.csv', dtype={'code': np.str}, parse_dates=True)
    yesterday = yesterday.set_index('code')
    holding = pd.read_csv('d:\\holding.csv',dtype={'code': np.str}, parse_dates=True)

    #sell
    cash = holding.cash[0]
    print cash
    for i in range(0, len(holding)):
        instrument = holding.ix[i]
        oneRicToday = today.loc[instrument.code]
        # trading
        if oneRicToday.open < 0.01:
            continue

        pos = yesterday.index.get_loc(instrument.code)
        row = yesterday.iloc[pos]
        ratio = oneRicToday.settlement / row.close
        # 1. Check 300
        if not pos < 300:
            amount = oneRicToday.open * instrument.validvol
            fee = amount * 0.0018 + instrument.validvol / 1000 * 0.6
            cash = cash + amount - fee
            holding.validvol[i] = 0
            print 'out 300 sell ' + instrument.code
            continue

        # 2. open high, but not reach limit, sell
        phigh = row.phigh * ratio
        highlimit = row.thighlimit * ratio
        if oneRicToday.open > phigh and oneRicToday.open < highlimit:
            fee = amount * 0.0018 + instrument.validvol / 1000 * 0.6
            cash = cash + amount - fee
            holding.validvol[i] = 0
            print 'open high sell ' + instrument.code
            continue
        # open less than alert line, sell
        hhigh = instrument.historyhigh * ratio
        if oneRicToday.open < round(hhigh * 1000 * 0.76, -1) / 1000:
            fee = amount * 0.0018 + instrument.validvol / 1000 * 0.6
            cash = cash + amount - fee
            holding.validvol[i] = 0
            print 'fallback sell ' + instrument.code
            continue
    holding = holding[holding.validvol > 0]
    #buy
    print cash
    cnt = len(holding)

    h300 = yesterday.head(300)
    valid = pd.DataFrame()
    valid['pclose'] = h300.close
    valid['open'] = today.open
    valid['settle'] = today.settlement
    valid['ratio'] = valid.settle / valid.pclose
    valid['name'] = today.name

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

    valid.to_csv('d:\\todayValid.csv', encoding='gbk')

    for i in range(0, 300):
        instrument = yesterday.ix[i]

    if cnt < 15:
        availablCnt = 15 - cnt
        margin = cash / availablCnt
        valid = valid[valid.buyflag == True]
        valid = valid.reset_index()
        for row in valid.itertuples():
            if availablCnt <= 0.01:
                break
            code = row.code
            open = row.open
            # can't buy at high limit
            if abs(row.open - row.highlimit) < 0.01:
                continue
            volume = int(margin / row.open / 100) * 100
            while (margin - volume*row.open - margin*0.00025 - volume/1000*0.6) < 0:
                volume = volume - 100
            if volume > 0:
                print 'buy ' + row.code + ' at price ' + str(row.open) + ' size ' + str(volume)
                availablCnt = availablCnt - 1

def getArgs():
    parse = argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['evening', 'morning'], default='morning', help='one of evening or morning')

    args=parse.parse_args()
    return vars(args)

def get_today_all():
    text = urlopen('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=8000&sort=mktcap&asc=0&node=hs_a').read()
    if text == 'null':
        return None
    reg = re.compile(r'\,(.*?)\:')
    text = reg.sub(r',"\1":', text.decode('gbk') if ct.PY3 else text)
    text = text.replace('"{symbol', '{"symbol')
    text = text.replace('{symbol', '{"symbol"')
    jstr = json.dumps(text, encoding='GBK')
    js = json.loads(jstr)
    return pd.DataFrame(pd.read_json(js, dtype={'code': object}))


if __name__ == "__main__":
    args = getArgs()
    type = args['t']

    if (type == 'evening'):
        generateYesterdayFile()
    elif (type == 'morning'):
        trade()
