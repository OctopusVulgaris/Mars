# -*- coding:utf-8 -*-
import pandas as pd
import requests
import threading
from lxml import etree
import datetime
import time
import csv
from time import ctime,clock

full_df = pd.DataFrame()
mutex = threading.Lock()

def get_open_price(code):
    ut = int(time.mktime(datetime.datetime.now().timetuple()))
    sut = str(ut)+'003'
    if code[0] == '6':
        url = r'http://hq.sinajs.cn/etag.php?_=' + sut + r'&list=sh' + code
    else:
        url = r'http://hq.sinajs.cn/etag.php?_=' + sut + r'&list=sz' + code

    for _ in range(60):
        try:
            content = requests.get(url, timeout=10).content
            s = content.split(',')
            #print code + '  ' + s[3]
            return s[1],s[2]
        except Exception as e:
            err = 'Error %s' % e
            #print('Error %s' % e)
            time.sleep(1)
        else:
            #print('get daily data for %s successfully' % row.code.encode("utf-8"))
            break
    print('get open price for %s failed' % code)

    #print price

def gop_worker(riclist, totalthreads, idx):
    sublist = riclist.loc[idx*10:300/30+idx*10-1]
    sublist.reset_index(inplace=True,drop=True)
    #print sublist.ix[0]['list']
    newcol = []
    newcol1 = []
    for row in sublist.itertuples():
        open,prev_cls = get_open_price(row[1])
        newcol.append(open)
        newcol1.append(prev_cls)
    sublist['open'] = newcol
    sublist['prevcls'] = newcol1
    #print sublist
    global full_df
    if mutex.acquire(60):
        full_df = full_df.append(sublist)
        mutex.release()

def get_ric_list():
    riclist = pd.read_csv('riclist.txt',dtype={'list':'|S6'})
    #print riclist
    return riclist

if __name__=="__main__":
    start = time.time()
    riclist = get_ric_list()
    threads = []
    totalthreads = 30
    for i in range(0,totalthreads,1):
        t = threading.Thread(target=gop_worker, args=(riclist,totalthreads,i,))
        threads.append(t)

    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()
    finish = time.time()
    print finish - start
    print full_df

    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    full_df['date'] = pd.Series(today, index=full_df.index)
    full_df[['list','open','prevcls','date']] = full_df[['date', 'list','open','prevcls']]
    full_df.columns = ['date','code','open','prevcls']
    #myfile = open('d:\\test1.csv', 'wb')
    #wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    #wr.writerow(full_df)
    full_df.to_csv("d:\\test1.csv",index=False)