# -*- coding:utf-8 -*-
import pandas as pd
import requests
import threading
from lxml import etree
import datetime
import time
from time import ctime,clock

def get_open_price(code):
    ut = int(time.mktime(datetime.datetime.now().timetuple()))
    sut = str(ut)+'003'
    if code[0] == '6':
        url = r'http://hq.sinajs.cn/etag.php?_=' + sut + r'&list=sh' + code
    else:
        url = r'http://hq.sinajs.cn/etag.php?_=' + sut + r'&list=sz' + code
    content = requests.get(url, timeout=10).content
    s = content.split(',')
    print code + '  ' + s[3]
    #print price

def gop_worker(riclist, totalthreads, idx):
    sublist = riclist.loc[idx*15:300/20+idx*15-1]
    sublist.reset_index(inplace=True)
    #print sublist.ix[0]['list']
    for row in sublist.itertuples():
        get_open_price(row[2])
    #print sublist

def get_ric_list():
    riclist = pd.read_csv('riclist.txt',dtype={'list':'|S6'})
    return riclist

if __name__=="__main__":
    start = time.time()
    riclist = get_ric_list()
    threads = []
    totalthreads = 20
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