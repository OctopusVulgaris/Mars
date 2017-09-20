# -*- coding:utf-8 -*-
import dataloader
from dataloader import engine
import time
import datetime
from lxml import etree
import requests
import re
import pandas as pd
import psycopg2
import logging
from sqlalchemy import Date, text, DateTime, Integer
from StringIO import StringIO
import unicodedata

conn = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port="5432")
cur = conn.cursor()

def write_fd_to_db(finfoarr):
    for finfo in finfoarr:
        order = "INSERT INTO public.fundamental(code, rdate, deps, weps, eps, roe, wroe)\
              VALUES (\'%s\',\'%s\', %s, %s, %s, %s, %s)" % (
                finfo['code'], finfo['rdate'], finfo['deps'], finfo['weps'], finfo['feps'], finfo['roe'], finfo['wroe'])
        try:
            cur.execute(order)
        except psycopg2.DatabaseError, e:
            err = 'Error %s' % e
            if (err.find('duplicate key value') != -1 or err.find('违反唯一约束') != -1):
                conn.rollback()
                return
            else:
                print err
                conn.rollback()
                conn.close()
        conn.commit()

def get_report_date(astrdate, type):
    adate = datetime.datetime.strptime(astrdate, '%Y-%m-%d')
    year = adate.year
    month = adate.month
    day = adate.day
    ryear = year
    rmonth = 0
    rday  = 0
    if (type == u'一季度报告'):
        rmonth = 3
        rday = 31
    elif (type == u'中期报告'):
        rmonth = 6
        rday = 30
    elif (type == u'三季度报告'):
        rmonth = 9
        rday = 30
    elif (type == u'年度报告'):
        rmonth = 12
        rday = 31
        if(month == 12 and day == 31):
            pass
        else:
            ryear -= 1
    else:
        return 0
    return datetime.datetime(ryear, rmonth, rday)
    #return str(ryear) + '-' + str(rmonth) + '-' + str(rday)

def get_fd_from_sina(code, timeout=10):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + r'/displaytype/4.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)

    table = selector.xpath('//*[@id="con02-1"]/table[1]')
    astr = ''.join(table[0].xpath('string(.)'))
    ylist = re.findall(r"\d+\.?\d*", astr)

    dfs = []
    finfoall = []
    for year in ylist:
        yurl = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + '/ctrl/'+ year + r'/displaytype/4.phtml'
        content = requests.get(yurl, timeout=timeout).content
        ct = content.decode('gbk')
        selector = etree.HTML(ct)

        rdates = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[1]/td')
        finfoarr = []
        for rdate in rdates[1::]:
            finfo = {}
            finfo['code'] = code
            finfo['rdate'] = date_time = datetime.datetime.strptime(''.join(rdate.xpath('text()')),'%Y-%m-%d')
            finfo['adate'] = '1900-1-1'
            finfoarr.append(finfo)

        depss = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[3]/td')
        itemtxt = ''.join(depss[0].xpath('a/text()'))
        if (itemtxt.find(u"摊薄每股收益") == -1):
            logging.error('can not get expected fundamental item of %s in %d' % code, year)
            break
        i = 0
        for deps in depss[1::]:
            finfoarr[i]['deps'] = ''.join(deps.xpath('text()'))
            if finfoarr[i]['deps'] == '--':
                finfoarr[i]['deps'] = '0'
            i += 1

        wepss = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[4]/td')
        itemtxt = ''.join(wepss[0].xpath('a/text()'))
        if (itemtxt.find(u"加权每股收益") == -1):
            logging.error('can not get expected fundamental item of %s in %d' % code, year)
            break
        i = 0
        for weps in wepss[1::]:
            finfoarr[i]['weps'] = ''.join(weps.xpath('text()'))
            if finfoarr[i]['weps'] == '--':
                finfoarr[i]['weps'] = '0'
            i += 1

        epss = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[5]/td')
        itemtxt = ''.join(epss[0].xpath('a/text()'))
        if (itemtxt.find(u"每股收益_调整后") == -1):
            logging.error('can not get expected fundamental item of %s in %d' % code, year)
            break
        i = 0
        for eps in epss[1::]:
            finfoarr[i]['eps'] = ''.join(eps.xpath('text()'))
            if finfoarr[i]['eps'] == '--':
                finfoarr[i]['eps'] = '0'
            i += 1

        roes = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[31]/td')
        itemtxt = ''.join(roes[0].xpath('a/text()'))
        if (itemtxt.find(u"净资产收益率") == -1):
            logging.error('can not get expected fundamental item of %s in %d' % code, year)
            break
        i = 0
        for roe in roes[1::]:
            finfoarr[i]['roe'] = ''.join(roe.xpath('text()'))
            if finfoarr[i]['roe'] == '--':
                finfoarr[i]['roe'] = '0'
            i += 1

        wroes = selector.xpath('//*[@id="BalanceSheetNewTable0"]/tbody/tr[32]/td')
        itemtxt = ''.join(wroes[0].xpath('a/text()'))
        if (itemtxt.find(u"加权净资产收益率") == -1):
            logging.error('can not get expected fundamental item of %s in %d' % code, year)
            break
        i = 0
        for wroe in wroes[1::]:
            finfoarr[i]['wroe'] = ''.join(wroe.xpath('text()'))
            if finfoarr[i]['wroe'] == '--':
                finfoarr[i]['wroe'] = '0'
            i += 1

        finfoall.extend(finfoarr)

    df = pd.DataFrame(finfoall, columns=['rdate', 'code', 'adate','deps', 'weps', 'eps', 'roe', 'wroe'])
    df = df.set_index('rdate')

    for seq in range(0, 100):
        url = r'http://quotes.money.163.com/f10/gsgg_' + code + r',dqbg,' + str(seq) + r'.html'
        content = requests.get(url, timeout=timeout).content
        ct = content.decode('utf-8')
        selector = etree.HTML(ct)

        nodatatxt = ''.join(selector.xpath('//*[@id="newsTabs"]/div/table/tr/td/text()'))
        if (nodatatxt.find(u"暂无数据") != -1):
            break

        rows = selector.xpath('//*[@id="newsTabs"]/div/table/tr')
        for row in rows:
            title = ''.join(row.xpath('td[1]/a/text()'))
            adate = ''.join(row.xpath('td[2]/text()'))
            rtype = ''.join(row.xpath('td[3]/text()'))
            rrdate = get_report_date(adate, rtype)
            if(rrdate != 0):
                try:
                    df.loc[rrdate.date(),'adate'] = adate
                except TypeError, e:
                    pass

    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/stockid/' + code + r'/page_type/yjdbg.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    txtlist = selector.xpath('//*[@id="con02-7"]/table[2]/tr/td[2]/div/ul/text()')

    for adate_sina in txtlist:
        listtmp = re.findall(r"\d+.?\d+.?\d*", adate_sina)
        if(len(listtmp) > 0):
            adate_sina = unicodedata.normalize('NFKD', listtmp[0]).encode('ascii', 'ignore')
            rrdate = get_report_date(adate_sina, u'一季度报告')
            if (rrdate != 0):
                try:
                    df.loc[rrdate.date(), 'adate'] = adate_sina
                except TypeError, e:
                    pass

    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/stockid/' + code + r'/page_type/zqbg.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    txtlist = selector.xpath('//*[@id="con02-7"]/table[2]/tr/td[2]/div/ul/text()')

    for adate_sina in txtlist:
        listtmp = re.findall(r"\d+.?\d+.?\d*", adate_sina)
        if (len(listtmp) > 0):
            adate_sina = unicodedata.normalize('NFKD', listtmp[0]).encode('ascii', 'ignore')
            rrdate = get_report_date(adate_sina, u'中期报告')
            if (rrdate != 0):
                try:
                    df.loc[rrdate.date(), 'adate'] = adate_sina
                except TypeError, e:
                    pass

    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/stockid/' + code + r'/page_type/sjdbg.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    txtlist = selector.xpath('//*[@id="con02-7"]/table[2]/tr/td[2]/div/ul/text()')

    for adate_sina in txtlist:
        listtmp = re.findall(r"\d+.?\d+.?\d*", adate_sina)
        if (len(listtmp) > 0):
            adate_sina = unicodedata.normalize('NFKD', listtmp[0]).encode('ascii', 'ignore')
            rrdate = get_report_date(adate_sina, u'三季度报告')
            if (rrdate != 0):
                try:
                    df.loc[rrdate.date(), 'adate'] = adate_sina
                except TypeError, e:
                    pass

    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_Bulletin/stockid/' + code + r'/page_type/ndbg.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    txtlist = selector.xpath('//*[@id="con02-7"]/table[2]/tr/td[2]/div/ul/text()')

    for adate_sina in txtlist:
        listtmp = re.findall(r"\d+.?\d+.?\d*", adate_sina)
        if (len(listtmp) > 0):
            adate_sina = unicodedata.normalize('NFKD', listtmp[0]).encode('ascii', 'ignore')
            rrdate = get_report_date(adate_sina, u'年度报告')
            if (rrdate != 0):
                try:
                    df.loc[rrdate.date(), 'adate'] = adate_sina
                except TypeError, e:
                    pass
    #write_fd_to_db(finfoarr)
    df['epsl4q'] = pd.Series(0.0, index=df.index)
    df['roel4q'] = pd.Series(0.0, index=df.index)
    iter = df.itertuples()
    try:
        row = next(iter)
        while row:
            year = row[0].year
            month = row[0].month
            day = row[0].day
            if(month == 12 and day == 31):
                epsl4q = row[5]
                roel4q = row[6]
            elif(month == 9 and day == 30):
                lastdec = datetime.datetime(year - 1, 12, 31)
                lastsep = datetime.datetime(year - 1, 9, 30)
                try:
                    ldeps = df.loc[lastdec, 'eps']
                    ldroe = df.loc[lastdec, 'roe']
                    lseps = df.loc[lastsep, 'eps']
                    lsroe = df.loc[lastsep, 'roe']
                except KeyError, e:
                    epsl4q = 0
                    roel4q = 0
                else:
                    if(row[5] != '0' and ldeps != '0' and lseps != '0'):
                        epsl4q = float(row[5]) + float(ldeps) - float(lseps)
                    else:
                        epsl4q = 0

                    if(row[6] != '0' and ldroe != '0' and lsroe != '0'):
                        roel4q = float(row[6]) + float(ldroe) - float(lsroe)
                    else:
                        roel4q = 0
            elif (month == 6 and day == 30):
                lastdec = datetime.datetime(year - 1, 12, 31)
                lastjun = datetime.datetime(year - 1, 6, 30)
                try:
                    ldeps = df.loc[lastdec, 'eps']
                    ldroe = df.loc[lastdec, 'roe']
                    ljeps = df.loc[lastjun, 'eps']
                    ljroe = df.loc[lastjun, 'roe']
                except KeyError, e:
                    epsl4q = 0
                    roel4q = 0
                else:
                    if (row[5] != '0' and ldeps != '0' and ljeps != '0'):
                        epsl4q = float(row[5]) + float(ldeps) - float(ljeps)
                    else:
                        epsl4q = 0

                    if (row[6] != '0' and ldroe != '0' and ljroe != '0'):
                        roel4q = float(row[6]) + float(ldroe) - float(ljroe)
                    else:
                        roel4q = 0
            elif (month == 3 and day == 31):
                lastdec = datetime.datetime(year - 1, 12, 31)
                lastmar = datetime.datetime(year - 1, 3, 31)
                try:
                    ldeps = df.loc[lastdec, 'eps']
                    ldroe = df.loc[lastdec, 'roe']
                    lmeps = df.loc[lastmar, 'eps']
                    lmroe = df.loc[lastmar, 'roe']
                except KeyError, e:
                    epsl4q = 0
                    roel4q = 0
                else:
                    if (row[5] != '0' and ldeps != '0' and lmeps != '0'):
                        epsl4q = float(row[5]) + float(ldeps) - float(lmeps)
                    else:
                        epsl4q = 0

                    if (row[6] != '0' and ldroe != '0' and lmroe != '0'):
                        roel4q = float(row[6]) + float(ldroe) - float(lmroe)
                    else:
                        roel4q = 0

            df.loc[row[0],'epsl4q'] = epsl4q
            df.loc[row[0], 'roel4q'] = roel4q
            row = next(iter)
    except StopIteration, e:
        pass
    df.to_sql('fundamental', engine, if_exists='append', dtype={'rdate': Date})

def get_fundamental(retry=50, pause=10):
    target_list = dataloader.get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_fd_from_sina(row.code.encode("utf-8"))
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    if (err.find('duplicate key value') != -1 or err.find('违反唯一约束') != -1):
                        break
                    else:
                        time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

    pass

if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='fundamental_log.txt'
                        )

    #get_fundamental()
    get_fd_from_sina('000034')
