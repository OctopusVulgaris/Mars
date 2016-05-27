# -*- coding:utf-8 -*-
from dataloader import engine
import time
import requests
import pandas as pd
from mydownloader import logging
from StringIO import StringIO

def get_stock_list(retry_count, pause):
    """
    get shanghai and shengkai stock list from their official website.
    Note: A rule has been set in db which will trigger update whenever any duplicate insert
    
    :param retry_count:
    :param pause: in sec
    :return:no
    """

    proxies = {
        'http': 'http://10.23.31.130:8080',
        'https': 'http://10.23.31.130:8080',
    }
    sz_onboard_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab2PAGENUM=1&ENCODE=1&TABKEY=tab2'
    sz_quit_onhold_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1793_ssgs&ENCODE=1&TABKEY=tab1'
    sz_quit_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1793_ssgs&ENCODE=1&TABKEY=tab2'

    sh_onboard_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1'
    sh_quit_onhold_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=4'
    sh_quit_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=5'

    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
        'Upgrade-Insecure-Request': '1'
    }

    for _ in range(retry_count):
        try:
            # get info of shen zheng
            r = requests.get(sz_onboard_url)  # , proxies=proxies)
            sz_on = pd.read_html(r.content)
            if sz_on:
                df2 = sz_on[0].iloc[1:, [0, 1, 18]]
                df2.columns = ['code', 'name', 'industry']
                df2['status'] = pd.Series(1, index=df2.index)
                df2.to_sql('stock_list', engine, if_exists='append', index=False)
                # for i in range(len(df2)):
                #     try:
                #         df2.iloc[i:i + 1].to_sql(name="stock_list", con=engine, if_exists='append', index=False)
                #     except Exception, e:
                #         err = 'Error %s' % e
                #         if (err.find('duplicate key value') > 0):
                #             continue

            r = requests.get(sz_quit_onhold_url)  # , proxies=proxies)
            sz_quit_onhold = pd.read_html(r.content)
            if sz_quit_onhold:
                df2 = sz_quit_onhold[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                df2['status'] = pd.Series(0, index=df2.index)
                df2.to_sql('stock_list', engine, if_exists='append', index=False)

            r = requests.get(sz_quit_url)  # , proxies=proxies)
            sz_quit = pd.read_html(r.content)
            if sz_quit:
                df2 = sz_quit[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                df2['status'] = pd.Series(-1, index=df2.index)
                df2.to_sql('stock_list', engine, if_exists='append', index=False)

            # get info of shang hai
            r = requests.get(sh_onboard_url, headers=header)  # , proxies=proxies,)
            #with open("sh_onboard.xls", "wb") as code:
            #    code.write(r.content)
            sh_on = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_on.empty:
                df1 = sh_on.iloc[0:, [2, 3]]
                df1.columns = ['code', 'name']
                df1['status'] = pd.Series(1, index=df1.index)
                df1.to_sql('stock_list', engine, if_exists='append', index=False)

            r = requests.get(sh_quit_onhold_url, headers=header)  # , proxies=proxies,)
            #with open("sh_quit_onhold.xls", "wb") as code:
            #    code.write(r.content)
            sh_onhold = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_onhold.empty:
                df1 = sh_onhold.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1['status'] = pd.Series(0, index=df1.index)
                df1.to_sql('stock_list', engine, if_exists='append', index=False)

            r = requests.get(sh_quit_url, headers=header)  # , proxies=proxies,)
            #with open("sh_quit.xls", "wb") as code:
            #    code.write(r.content)
            sh_quit = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_quit.empty:
                df1 = sh_quit.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1['status'] = pd.Series(-1, index=df1.index)
                df1.to_sql('stock_list', engine, if_exists='append', index=False)
        except Exception as e:
            err = 'Error %s' % e
            logging.info(err)
            time.sleep(pause)
        else:
            logging.info('get_stock_list finished successfully')
            return
    logging.info('get_stock_list failed')

if __name__=="__main__":
    get_stock_list(5, 5)












