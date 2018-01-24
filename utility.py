# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import json
import re
import tushare as ts
import threading
import time
from urllib.request import urlopen
import subprocess as sp
import sys
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import socket
import configparser
import datetime as dt

st_pattern = r'^S|^\*|退|ST'
def reconnect():
    sp.call('rasdial 宽带连接 /disconnect', stdout=sys.stdout)
    time.sleep(1)
    sp.call('rasdial 宽带连接 *63530620 040731', stdout=sys.stdout)

full_df = pd.DataFrame()
mutex = threading.Lock()

Rmin = lambda x: (x / x[-1]).min()
Rmax = lambda x: (x / x[-1]).max()
R= lambda x: x[0] / x[-1]

def slope(day, Y):
    X = np.array(range(1, day+1))/10
    return ((X * Y).mean() - X.mean() * Y.mean()) / ((X ** 2).mean() - (X.mean()) ** 2)

def calcXdaySlope(x, days):
    y = x.sort_index(level=1, ascending=False)
    #as the sequence reversed, so slope need * -1
    y = y.rolling(window=days).apply(lambda x: slope(days, x)*-1)
    return y.sort_index(level=1, ascending=True)

def calcXday(x, days, foo):
    y = x.sort_index(level=1, ascending=False)
    y = y.rolling(window=days).apply(foo)
    return y.sort_index(level=1, ascending=True)

def calcXdayMean(x, days):
    y = x.sort_index(level=1, ascending=False)
    y = y.rolling(window=days).mean()
    return y.sort_index(level=1, ascending=True)

def calcXdayStd(x, days):
    y = x.sort_index(level=1, ascending=False)
    y = y.rolling(window=days).std()
    return y.sort_index(level=1, ascending=True)


def round_series(s):
    s = s * 1000
    s = s.apply(round, ndigits=-1)
    return s / 1000

def get_today_all():
    text = urlopen('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=8000&sort=mktcap&asc=0&node=hs_a&symbol=&_s_r_a=page&page=0').read().decode("utf8")
    if text == 'null':
        return None
    reg = re.compile(r'\,(.*?)\:')
    text = reg.sub(r',"\1":', text.decode('gbk'))
    text = text.replace('"{symbol', '{"symbol')
    text = text.replace('{symbol', '{"symbol"')
    jstr = json.dumps(text, encoding='GBK')
    js = json.loads(jstr)
    return pd.DataFrame(pd.read_json(js, dtype={'code': object}))

def gop_worker(symbols, idx, retry=60):
    #length = len(symbols)
    #if(idx*200 > length):
    #    return
    sublist = symbols[idx*300:(idx+1)*300]
    for _ in range(retry):
        try:
            df = ts.get_realtime_quotes(sublist)
        except Exception as e:
            err = 'Error %s' % e
            # print('Error %s' % e)
            time.sleep(1)
        else:
            # print('get daily data for %s successfully' % row.code.encode("utf-8"))
            break
    global full_df
    if mutex.acquire(60):
        full_df = full_df.append(df)
        mutex.release()

def getcodelist(active=False):
    df = pd.read_hdf('d:/HDF5_Data/stocklist.hdf').reset_index()
    if active:
        return df[df.status>0]
    return df

def getindexlist():
    return pd.read_hdf('d:/HDF5_Data/indexlist.hdf')

def get_realtime_all():
    start = time.time()
    riclist = getcodelist()
    length = len(riclist)
    symbols =  riclist['code']
    threads = []
    totalthreads = length / 300 + 1
    for i in range(0, totalthreads, 1):
        t = threading.Thread(target=gop_worker, args=(symbols, i,))
        threads.append(t)

    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()
    finish = time.time()
    print (finish - start)
    full_df[['name','open','pre_close','price','date','code']] = full_df[['date','code','open','pre_close','price','name']]
    names = full_df.columns.tolist()
    names[names.index('name')] = 'Date'
    names[names.index('open')] = 'Code'
    names[names.index('pre_close')] = 'Open'
    names[names.index('price')] = 'Pre_close'
    names[names.index('date')] = 'Price'
    names[names.index('code')] = 'Name'
    full_df.columns = names
    #full_df.to_csv('d:\\ut.csv',encoding='utf-8',index=False)
    return full_df

def get_realtime_all_st(symbols=[], retry=60):
    start = time.time()
    if len(symbols) < 1:
        riclist = getcodelist(True)
        symbols = riclist['code'].values
    full_df = pd.DataFrame()
    length = len(symbols)
    loops = int(length / 300 + 1)
    for idx in range(0, loops, 1):
        sublist = symbols[idx * 300:(idx + 1) * 300]
        for _ in range(retry):
            try:
                df = ts.get_realtime_quotes(sublist.tolist())
            except Exception as e:
                err = 'Error %s' % e
                print('Error %s' % e)
                time.sleep(1)
            else:
                # print('get daily data for %s successfully' % row.code.encode("utf-8"))
                break
        full_df = full_df.append(df)
    finish = time.time()
    print (finish - start)
    full_df.date = pd.to_datetime(full_df.date)
    full_df.open = full_df.open.astype(np.float64)
    full_df.pre_close = full_df.pre_close.astype(np.float64)
    full_df.price = full_df.price.astype(np.float64)
    full_df.high = full_df.high.astype(np.float64)
    full_df.low = full_df.low.astype(np.float64)
    full_df.volume = full_df.volume.astype(np.int64)
    full_df.amount = full_df.amount.astype(np.float64)

    return full_df

def sendmail(log, prjname):
    config = configparser.ConfigParser()
    config.read('d:\\tradelog\\mail.ini')

    fromaddr = config.get('mail', 'from')
    toaddr = config.get('mail', 'to')
    password = config.get('mail', 'pw')
    msg = MIMEText(log, 'plain')
    msg['Subject'] = Header('%s@' % (prjname) + str(dt.date.today())  + '_' + socket.gethostname())
    msg['From'] = fromaddr
    msg['To'] = toaddr

    try:
        sm = smtplib.SMTP_SSL('smtp.qq.com')
        sm.ehlo()
        sm.login(fromaddr, password)
        sm.sendmail(fromaddr, toaddr.split(','), msg.as_string())
        sm.quit()
    except Exception as e:
        logging.error(str(e))

if __name__=="__main__":
    df = get_realtime_all_st()
    df.to_csv('D:\\Tool\\RealTimeStrategy\\x64\\Release\\data\\today.csv', encoding='utf-8', index=False)
    #df = get_today_all()
    #df.to_csv('d:\\ut_all.csv',encoding='utf-8',index=False)