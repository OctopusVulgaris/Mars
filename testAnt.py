# -*- coding:utf-8 -*-

import pandas as pd
import tushare as ts
import numpy as np
import json
import re
import requests
from lxml import etree
from StringIO import StringIO

import argparse
import utility
import datetime
import time
import logging
import threading

from dataloader import engine, get_code_list


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='testAnt.log'
                    )

def get_bonus_and_ri(code, brsStore, timeout=5):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'+ code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
    ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
    retry = 0
    while (len(bitems) < 1 or len(ritems) < 1) and retry < 10:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
        ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
        retry += 1
        logging.info('Info retrying %d" bonus' % retry)
    dfs = []
    dfs1 = []
    for item in bitems:
        binfo = {}

        binfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if(binfo['adate'].find(u'没有数据') > 0):
            logging.info('Info %s "没有数据" bonus' % code)
            break
        if(binfo['adate'] == '--'):
            binfo['adate'] = '1900-1-1'
        binfo['code'] = code
        binfo['give'] = ''.join(item.xpath('td[2]/text()'))
        binfo['trans'] = ''.join(item.xpath('td[3]/text()'))
        binfo['divpay'] = ''.join(item.xpath('td[4]/text()'))
        binfo['xdate'] = ''.join(item.xpath('td[6]/text()'))
        if(binfo['xdate'] == '--'):
            binfo['xdate'] = '1900-1-1'
        binfo['rdate'] = ''.join(item.xpath('td[7]/text()'))
        if (binfo['rdate'] == '--'):
            binfo['rdate'] = '1900-1-1'
        #write_bonus_to_db(binfo)
        df = pd.DataFrame()
        df = df.from_dict(binfo, orient='index')
        dfs.append(df.T)

    for item in ritems:
        rinfo = {}

        rinfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if (rinfo['adate'].find(u'没有数据') > 0):
            logging.info('Info %s "没有数据" rightsissue' % code)
            break
        if (rinfo['adate'] == '--'):
            rinfo['adate'] = '1900-1-1'
        rinfo['ri'] = ''.join(item.xpath('td[2]/text()'))
        rinfo['riprice'] = ''.join(item.xpath('td[3]/text()'))
        rinfo['basecap'] = ''.join(item.xpath('td[4]/text()'))
        rinfo['xdate'] = ''.join(item.xpath('td[5]/text()'))
        rinfo['rdate'] = ''.join(item.xpath('td[6]/text()'))
        if (rinfo['xdate'] == '--'):
            rinfo['xdate'] = '1900-1-1'
        if (rinfo['rdate'] == '--'):
            rinfo['rdate'] = '1900-1-1'
        rinfo['code'] = code
        # write_ri_to_db(rinfo)
        df = pd.DataFrame()
        df = df.from_dict(rinfo, orient='index')
        dfs1.append(df.T)

    if len(dfs) > 0:
        df = pd.concat(dfs)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.rdate = pd.to_datetime(df.rdate)
        df.give = df.give.astype(np.float64)
        df.trans = df.trans.astype(np.float64)
        df.divpay = df.divpay.astype(np.float64)
        df['type'] = 'bonus'
        #print df
        key = 'b' + code
        brsStore.append('bonus', df, min_itemsize={'values': 50})
        logging.info('Info %s has saved bonus' % code)
        #df.to_hdf('d:\\HDF5_Data\\binfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty bonus' % code)
        logging.info('Info len bitems %d' % len(bitems))

    if len(dfs1) > 0:
        df1 = pd.concat(dfs1)
        df1.adate = pd.to_datetime(df1.adate)
        df1.xdate = pd.to_datetime(df1.xdate)
        df1.rdate = pd.to_datetime(df1.rdate)
        df1.ri = df1.ri.astype(np.float64)
        df1.riprice = df1.riprice.astype(np.float64)
        df1.basecap = df1.basecap.astype(np.float64)
        df1['type'] = 'rightsissue'
        #print df
        key = 'r' + code
        brsStore.append('rightsissue', df1, min_itemsize = {'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\rinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty righsissue' % code)
        logging.info('Info len ritems %d' % len(ritems))


def is_digit_or_point(c):
    if(str.isdigit(c)):
        return True
    elif(c == '.'):
        return True
    else:
        return False

def get_stock_change(code, brsStore, timeout=5):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/' + code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    tables = selector.xpath('//*[@id="con02-1"]/table')
    retry = 0
    while len(tables) < 1 and retry < 10:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        tables = selector.xpath('//*[@id="con02-1"]/table')
        retry += 1
        logging.info('Info retrying %d" stock change' % retry)

    prev_tradeable_share = 0.0
    dfs = []
    for table in tables[::-1]:
        cols = table.xpath('tbody/tr[1]/td')
        for col in range(len(cols), 1, -1):
            sinfo = {}
            sinfo['code'] = code
            sinfo['xdate'] = ''.join(table.xpath('tbody/tr[1]/td[%d]/text()'%col))
            if (sinfo['xdate'] == ''):
                sinfo['xdate'] = '1900-1-1'
            sinfo['adate'] = ''.join(table.xpath('tbody/tr[2]/td[%d]/text()'%col))
            if (sinfo['adate'] == ''):
                sinfo['adate'] = '1900-1-1'
            sinfo['reason'] = ''.join(table.xpath('tbody/tr[4]/td[%d]/text()'%col))
            sinfo['totalshare'] = ''.join(table.xpath('tbody/tr[5]/td[%d]/text()'%col))
            sinfo['totalshare'] = filter(is_digit_or_point, sinfo['totalshare'].encode('utf-8'))
            sinfo['tradeshare'] = ''.join(table.xpath('tbody/tr[7]/td[%d]/text()'%col))
            sinfo['tradeshare'] = filter(is_digit_or_point, sinfo['tradeshare'].encode('utf-8'))
            sinfo['limitshare'] = ''.join(table.xpath('tbody/tr[9]/td[%d]/text()'%col))
            sinfo['limitshare'] = filter(is_digit_or_point, sinfo['limitshare'].encode('utf-8'))
            sinfo['prevts'] = prev_tradeable_share
            prev_tradeable_share = sinfo['tradeshare']
            #write_stockchange_to_db(sinfo)
            df = pd.DataFrame()
            df = df.from_dict(sinfo, orient='index')
            dfs.append(df.T)
    if len(dfs) > 0:
        df = pd.DataFrame()
        df = pd.concat(dfs)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.totalshare = df.totalshare.astype(np.float64)
        df.tradeshare = df.tradeshare.astype(np.float64)
        df.limitshare = df.limitshare.astype(np.float64)
        df.prevts = df.prevts.astype(np.float64)
        df.reason = df.reason.str.encode('utf-8')
        df['type'] = 'stockchange'
        #print df
        key = 's' + code
        brsStore.append('stockchange', df, min_itemsize={'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\sinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty stock change' % code)
        logging.info('Info len sitems %d' % len(tables))

def get_bonus_ri_sc(retry=50, pause=1):
    brsStore = pd.HDFStore('D:\\HDF5_Data\\brsInfo.h5', complib='blosc', mode='w')
    target_list = get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print 'retrieving bonus_and_ri' + row.code.encode("utf-8")
                    get_bonus_and_ri(row.code.encode("utf-8"), brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s bonus_and_ri data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print 'retrieving stock change' + row.code.encode("utf-8")
                    get_stock_change(row.code.encode("utf-8"), brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s stock change data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def convertNone(c):
    if(c == 'None' or c == 'null' or c== 'NULL'):
        return float(0.00)
    else:
        return float(c)

def get_stock_full_daily_data(code, daykStore, timeout=3):
    if code[0] == '6':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap']
        data['code'] = pd.Series(code, index=data.index)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['turnoverrate'] = data['turnoverrate'].apply(convertNone)
        data['nameutf'] = 'utf8'
        data.nameutf = data.name.str.encode('utf-8')
        del data['name']
        data.columns = ['code', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng',
                        'turnoverrate', 'vol', 'amo', 'totalcap', 'tradeablecap', 'name']
        data['hfqratio'] = 1.0
        data = data.reset_index()
        data = data.set_index(['code', 'date'])
#        data.to_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
        data = data.sort_index()
        daykStore.append('dayk', data, min_itemsize={'values': 30})


def get_all_full_daily_data(retry=50, pause=1):
    daykStore = pd.HDFStore('D:\\HDF5_Data\\dailydata.h5', complib='blosc', mode='w')
    target_list = get_code_list('', '', engine)
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_stock_full_daily_data(row.code.encode("utf-8"), daykStore)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code.encode("utf-8"))
                    cnt += 1
                    print 'retrieved ' + row.code.encode("utf-8") + ', ' + str(cnt) + ' of ' + str(llen)
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def close2PrevClose(x):
    r = pd.Series(0, x.index)
    x.close.replace('0', inplace=True)
    r[1:] = x.ix[:len(x) - 1].close.values
    return r.reset_index(level=0, drop=True)

def cumprod(x):
    return x.cumprod()

def calc():
    t1 = datetime.datetime.now()
    dayk = pd.HDFStore('d:\\HDF5_Data\\dailydata.h5', mode='r')
    brs = pd.HDFStore('d:\\HDF5_Data\\brsInfo.h5', mode='r')
    df = dayk['dayk']
    bi = brs['bonus']
    ri = brs['rightsissue']
    si = brs['stockchange']
    dayk.close()
    brs.close()

    df = df.sort_index()
    bi = bi[bi.xdate > '1990-1-1']
    bi = bi.reset_index(drop=True).groupby(['code', 'xdate']).sum()
    if not bi.index.is_unique:
        raise IndexError('bonus index is not unique')
    ri = ri[ri.xdate > '1990-1-1']
    ri = ri.set_index(['code', 'xdate'])
    if not ri.index.is_unique:
        raise IndexError('rightsissue index is not unique')
    si = si[si.xdate > '1990-1-1']
    si = si[(si.reason == '股权分置') | (si.reason == '拆细')].set_index(['code', 'xdate'])
    si = si[(si.tradeshare > 0) & (si.prevts > 0)]
    if not si.index.is_unique:
        raise IndexError('stockchange index is not unique')
    #merge bi, ri
    sPclose = df.groupby(level=0).apply(close2PrevClose)
    bi['pclose'] = sPclose.reindex(bi.index, method='pad')
    bi['riprice'] = ri.riprice
    bi['ri'] = ri.ri
    bi = bi.fillna(0)
    print datetime.datetime.now() - t1

    #give + divpay + rightsissue
    factor = (bi.give + bi.trans) / 10 + bi.divpay / (bi.pclose - bi.divpay) + bi.pclose * (1 + bi.ri / 10) / (bi.pclose + bi.riprice * bi.ri / 10)

    t2 = datetime.datetime.now()
    bsIndex = factor.index.union(si.index)
    factor = factor.reindex(bsIndex, fill_value=0)
    factor = si.tradeshare / si.prevts
    combinedIndex = df.index.union(factor.index)
    df = df.reindex(combinedIndex, fill_value=0)
    df.hfqratio = factor
    df.hfqratio.fillna(1, inplace=True)
    df.hfqratio = df.hfqratio.groupby(level=0).apply(cumprod)
    print datetime.datetime.now() - t2
    return df



def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['full', 'delta','bonus','bnd'], default='bonus', help='download type')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']
    get_bonus_ri_sc()
    get_all_full_daily_data()




