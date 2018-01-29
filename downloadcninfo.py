# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
import subprocess as sp
import sys
import requests
from lxml import etree
from io import StringIO, BytesIO
import random, string
from utility import round_series, getcodelist, getindexlist, reconnect
import argparse
import logging

def random_str(randomlength=8):
    a = list(string.ascii_letters)
    random.shuffle(a)
    return ''.join(a[:randomlength])

def getcninfooneric(code, startyear, endyear, type):
    #reconnect()
    if code[0] == '6':
        exchange = 'sh'
    else:
        exchange = 'sz'

    BOUNDARY = "----WebKitFormBoundary" + random_str(16)
    fields = [['K_code', ''],
         ['market', exchange],
         ['type', type],
         ['code', code],
         ['orgid', 'gs' + exchange + code],
         ['minYear', startyear],
         ['maxYear', endyear],
         ['hq_code', code],
         ['hq_k_code', ''],
         ['cw_code', code],
         ['cw_k_code', '']]
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '1107',
        'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY,
        # 'Cookie':'JSESSIONID=EDCC00C060A5E9338635CE524C7305FC',
        'Host': 'www.cninfo.com.cn',
        'Origin': 'http://www.cninfo.com.cn',
        'Proxy-Connection':'keep-alive',
        'Referer': 'http://www.cninfo.com.cn/cninfo-new/index',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    }
    r = requests.post("http://www.cninfo.com.cn/cninfo-new/data/download", data=body, headers=headers)

    df = pd.DataFrame()
    if zipfile.is_zipfile(BytesIO(r.content)):
        data = zipfile.ZipFile(BytesIO(r.content))
        data.extractall('d:/cninfo/%s' % type)
        logging.info('finish %s %d' % (code, len(data.filelist)))
        #return len(data.namelist())
    else:
        logging.info('not zip %s' % (code))
        raise ValueError
    return 0

def task(type):
    cl = getcodelist()
    a = cl.code.apply(lambda x: 'python D:/project/OctopusVulgaris/Mars/downloadcninfo.py -c %s -s 2018 -e 2018 -t %s' % (x, 'hq'))
    parts = 1
    l = len(a)
    all = []
    for i in range(parts):
        excutefile = 'D:/project/OctopusVulgaris/Mars/excute/getcninfo_%d.bat'%i
        a[i*int(l/parts):(i+1)*int(l/parts)].to_csv(excutefile, index=False)
        all.append('start %s\n'% excutefile )

    fp = open('D:/project/OctopusVulgaris/Mars/excute/getcninfo.bat', 'w')
    fp.writelines(all)
    fp.close()

def csvtohdf(type):
    t1 = time.clock()
    inputdir = 'd:/cninfo/%s/' % (type)
    outputfile = 'd:/hdf5_data/cninfo_%s.hdf'% (type)
    #daykStore = pd.HDFStore('D:/HDF5_Data/dailydata_cninfo.h5', complib='blosc', mode='w')
    dfs = []
    all = os.listdir(inputdir)
    for file in all:
        csv = pd.read_csv(inputdir+file, encoding='gbk', parse_dates=[2], names=['code', 'name', 'date', 'exchange', 'preclose', 'open', 'vol', 'high', 'low', 'close', 'tradecnt', 'pctchange', 'amo'], header=0, converters={'code': lambda s: s.strip('\t')}, dtype=float)
        if csv.empty or pd.isna(csv.name[0]):
            continue
        csv = csv.fillna(0)
        csv['code'] = csv.code.astype(str)
        #print(file)
        dfs.append(csv)
        #daykStore.append('day', csv, min_itemsize={'name': 20, 'exchange': 15})
    #daykStore.close()
    t2 = time.clock()
    print(t2-t1)
    df = pd.concat(dfs)
    t3 = time.clock()
    print(t3-t2)
    df.to_hdf(outputfile, 'day', mode='w', format='t')
    t4 = time.clock()
    print(t4 - t3)

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-c', type=str, default='000001')
    parse.add_argument('-s', type=str, default=2018)
    parse.add_argument('-e', type=str, default=2018)
    parse.add_argument('-t', type=str, default='hq')
    parse.add_argument('-T', type=str, default='None')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/downloadcninfo.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    args = getArgs()
    code = args['c']
    start = args['s']
    end = args['e']
    type = args['t']
    T = args['T']
    if 'hq' == T:
        task(task)
    elif 'csvtohdf' == T:
        csvtohdf('hq')
    else:
        for _ in range(10):
            try:
                logging.info('start %s' % code)
                getcninfooneric(code, start, end, type)
            except Exception:
                logging.info('retry on %s' % code)
                continue
            else:
                break
