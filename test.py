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
import datetime as dt
from lxml import etree
from io import StringIO, BytesIO
import random, string
from utility import round_series, getcodelist, getindexlist, reconnect
import argparse

def getadate(astrdate):
    try:
        adate = dt.datetime.strptime(astrdate, '%Y-%m-%d')
    except ValueError:
        return dt.datetime(1900, 1, 1)
    else:
        return adate

def getreportdate(astrdate, type):
    if(astrdate == ''):
        astrdate = '1900-1-1'
    try:
        adate = dt.datetime.strptime(astrdate, '%Y-%m-%d')
    except ValueError:
        return dt.datetime(1900, 1, 1)
    year = adate.year
    month = adate.month
    day = adate.day
    ryear = year
    rmonth = 0
    rday  = 0
    if (type == '一季度报告'):
        rmonth = 3
        rday = 31
    elif (type == '中期报告'):
        rmonth = 6
        rday = 30
    elif (type == '三季度报告'):
        rmonth = 9
        rday = 30
    elif (type == '年度报告'):
        rmonth = 12
        rday = 31
        if(month == 12 and day == 31):
            pass
        else:
            ryear -= 1
    else:
        return dt.datetime(1900, 1, 1)
    return dt.datetime(ryear, rmonth, rday)

report_type = {
    '一季度报告' : 'yjdbg',
    '中期报告'  : 'zqbg',
    '三季度报告' : 'sjdbg',
    '年度报告': 'ndbg'
}
def getadate163(code, maxseq = 100):

    print(code)
    codes = []
    adates = []
    rdates = []

    for seq in range(0, maxseq):
        for _ in range(10):
            try:
                url = 'http://quotes.money.163.com/f10/gsgg_' + code + ',dqbg,' + str(seq) + '.html'
                content = requests.get(url, timeout=5).content
                ct = content.decode(encoding='utf-8', errors='ignore')
                selector = etree.HTML(ct)
                nodatatxt = ''.join(selector.xpath('//*[@id="newsTabs"]/div/table/tr/td/text()'))
                break
            except:
                reconnect()
                continue
            else:
                break

        if (nodatatxt.find("暂无数据") != -1):
            break
        rows = selector.xpath('//*[@id="newsTabs"]/div/table/tr')
        for row in rows:
            stradate = ''.join(row.xpath('td[2]/text()'))
            rtype = ''.join(row.xpath('td[3]/text()'))
            rdate = getreportdate(stradate, rtype)
            adate = getadate(stradate)
            codes.append(code)
            adates.append(adate)
            rdates.append(rdate)


    df = pd.DataFrame(data={'code':codes, '报告日期':rdates, '公告日期':adates})
    df = df.drop_duplicates(['code', '报告日期'], keep='last')
    df = df.set_index(['code', '报告日期']).sort_index()
    if not df.empty:
        df = df.loc(axis=0)[:, '1990-1-1':]
    #print(df)
    return df

def getadate163all():
    target_list = getcodelist()
    dfs = []
    for code in target_list.code.values:
        dfs.append(getadate163(code, 100))

    all = pd.concat(dfs)
    #print(all.to_string())
    all.to_hdf('d:/hdf5_data/adate163.hdf', 'fundamental')

def getadatesina(code, type):

    print(code)
    codes = []
    adates = []
    rdates = []

    url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_BulletinYi/stockid/' + code + '/page_type/' + report_type[type] + '.phtml'

    for _ in range(10):
        try:
            content = requests.get(url, timeout=5).content
            ct = content.decode(encoding='gbk', errors='ignore')
            selector = etree.HTML(ct)

            nodatatxt = ''.join(selector.xpath('//*[@id="con02-7"]/table/tr/td/text()'))
            if (nodatatxt.find("暂时没有数据") != -1):
                return pd.DataFrame()

            itr = selector.xpath('//*[@class="datelist"]/ul')[0].itertext()
        except:
            reconnect()
        else:
            break

    try:
        while True:
            txt = next(itr).strip()
            rdate = getreportdate(txt, type)
            adate = getadate(txt)
            codes.append(code)
            adates.append(adate)
            rdates.append(rdate)
    except StopIteration:
        pass

    df = pd.DataFrame(data={'code':codes, '报告日期':rdates, '公告日期':adates})
    df = df.drop_duplicates(['code', '报告日期'], keep='last')
    df = df.set_index(['code', '报告日期']).sort_index()
    if not df.empty:
        df = df.loc(axis=0)[:, '1990-1-1':]

    return df

def getadatesinaall():
    target_list = getcodelist()
    dfs = []
    for code in target_list.code.values:
        dfs.append(getadatesina(code, '一季度报告'))
        dfs.append(getadatesina(code, '中期报告'))
        dfs.append(getadatesina(code, '三季度报告'))
        dfs.append(getadatesina(code, '年度报告'))

    all = pd.concat(dfs)
    all.to_hdf('d:/hdf5_data/adatesina.hdf', 'fundamental')

def readsinapage(url, match='.+', att=None):
    for _ in range(10):
        try:
            pages = pd.read_html(url, encoding='gbk', match=match, attrs=att)
            if len(pages) < 1:
                reconnect()
            else:
                return pages
        except Exception:
            reconnect()

def getfundamentalsinaric(code):
    print(code)
    yr = dt.date.today().year - 1
    url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + '/ctrl/' + str(yr) + '/displaytype/4.phtml'
    page = readsinapage(url, match='历年数据')

    years = page[0].iloc[0].values[0].strip('历年数据: ').split(' ')

    for year in years:
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + '/ctrl/' + str(year) + '/displaytype/4.phtml'
        page = readsinapage(url, att={'id':'BalanceSheetNewTable0'})
        page[0].dropna().T.replace('--', np.nan).to_csv('D:/HDF5_Data/fundamental/sina/cwzb/'+code+'_'+str(year)+'.csv', header=False, index=False, encoding='gbk')


def getfundamentalsinaall():
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    for code in target_list.code.values:
        getfundamentalsinaric(code)


def csvtohdf(source, type):
    t1 = time.clock()
    inputdir = 'd:/hdf5_data/fundamental/%s/%s/' % (source, type)
    outputfile = 'd:/hdf5_data/fundamental%s_%s.hdf'% (source, type)
    dfs = []
    all = os.listdir(inputdir)
    for file in all:
        print(file + str())
        try:
            csv = pd.read_csv(inputdir+file, encoding='gbk', parse_dates=['报告日期'], header=0, dtype=float)
            year = csv['报告日期'].dt.year.max()
            csv = csv.set_index('报告日期')
            idx = pd.DatetimeIndex([dt.datetime(year, 3, 31, 0, 0, 0), dt.datetime(year, 6, 30, 0, 0, 0), dt.datetime(year, 9, 30, 0,0,0), dt.datetime(year,12,31,0,0,0)])
            csv = csv.reindex(idx)
            #csv['年度报告'] = np.nan
            #csv.loc[dt.datetime(year, 12, 31, 0, 0, 0), '年度报告'] = csv['每股收益_调整后(元)'][-1]
            csv['code'] = file[:6]
            csv = csv.set_index(['code', csv.index])
            csv = csv.sort_index().rolling(window=2).apply(lambda x: x[1] - x[0]).combine_first(csv)
        except ValueError:
            pass
        else:
            dfs.append(csv)
    t2 = time.clock()
    print(t2-t1)
    df = pd.concat(dfs)
    t3 = time.clock()
    print(t3-t2)
    df = df.sort_index()
    df.to_hdf(outputfile, 'fundamental')
    t4 = time.clock()
    print(t4 - t3)

def processfundamental():
    fdsina = pd.read_hdf('d:/hdf5_data/fundamentalsina_cwzb.hdf')
    eps = fdsina['每股收益_调整后(元)']
    epsttm = eps.groupby(level=0, group_keys=False).rolling(window=4).sum()
    epsyoy = eps.groupby(level=0).resample('Y', level=1).sum()
    epsall = epsttm.combine_first(epsyoy)
    epsall = epsall.loc(axis=0)[:, '2007-1-1':].dropna()
    epsall = pd.DataFrame(epsall)
    adate = pd.read_hdf('d:/hdf5_data/adatesina.hdf')
    adate163 = pd.read_hdf('d:/hdf5_data/adate163.hdf')
    adate = adate.combine_first(adate163)
    adate = pd.DataFrame(adate)
    epsall['adate'] = adate
    epsall = epsall.sort_index(ascending=True)
    epstmp = epsall.drop_duplicates('adate', keep='last')
    epstmp = epstmp.dropna().reset_index().set_index(['code', 'adate']).sort_index()

    day = pd.read_hdf('d:/hdf5_data/dailydata.h5', columns=['open'])
    #epstmp = epstmp.reindex(day.index, method='ffill')
    combinedIndex = epstmp.index.union(day.index)
    epstmp = epstmp.reindex(combinedIndex)
    epstmp = epstmp.groupby(level=0).fillna(method='ffill')

    combinedIndex = epsall.index.union(day.index)
    epsall = epsall.reindex(combinedIndex)
    epsall = epsall.groupby(level=0).fillna(method='ffill')


    epsall = epstmp['每股收益_调整后(元)'].combine_first(epsall['每股收益_调整后(元)'])
    day['eps'] = epsall
    day.to_hdf('d:/hdf5_data/dailydata.h5','day')


#getoneric163fundamental('000001')
#getadate163all()
#csvtohdf('sina', 'cwzb')
#getfundamentalsinaall()
processfundamental()