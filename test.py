# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import logging
import threading
import dataloader
import numpy as np
import re

logging.basicConfig(filename='test_log.txt', level=logging.DEBUG)
from sqlalchemy import Date, text, DateTime, Integer

datepattern = r'\[(\d*\-\d*\-\d*)\]'
buypattern = r'Buy (\d{6}).* on (.*\d), at (.*) vol (.*) fee (.*) avaCap.*'
sellpattern = r'Sell (\d{6}).* on (.*), at (.*) ER.* amount (.*) profit.* fee (.*) Avacap.*'
ifilepath = 'd:\\sc.txt'
ofilepath = 'd:\\sc.csv'
myfilepath = 'd:\\transaction_log.csv'
ifile = open(ifilepath)
ofile = open(ofilepath, mode='w')

line = ifile.readline()
oline = ''
olines = []
olines.append('date,type,code,price,amount,fee\n')
while line:
    m = re.search(buypattern, line)
    try:
        if m:
            date = datetime.datetime.strptime(m.groups()[1], '%Y-%m-%d')
            sdate = date.strftime('%m/%d/%Y')
            oline = sdate + ',buy,' + m.groups()[0] + ',' + m.groups()[2] + ',' + str(float(m.groups()[2]) * float(m.groups(

            )[3])) +',' + m.groups()[4] + '\n'
            olines.append(oline)
            line = ifile.readline()
            continue
        m = re.search(sellpattern, line)
        if m:
            date = datetime.datetime.strptime(m.groups()[1], '%Y-%m-%d')
            sdate = date.strftime('%m/%d/%Y')
            oline = sdate + ',sell,' + m.groups()[0] + ',' + m.groups()[2] + ',' + m.groups()[3] + ',' + m.groups()[4] + '\n'
            olines.append(oline)
            line = ifile.readline()
            continue
    except Exception:
        print m.groups()
    line = ifile.readline()

ofile.writelines(olines)
ifile.close()
ofile.close()

def s(x):
    return x.sort_values('code')
df = pd.read_csv(ofilepath, index_col='date', parse_dates=True)
df = df.groupby(level=0).apply(s).reset_index(0, drop=True).sort_index().reset_index()
df = df.drop_duplicates(keep='first')
df.to_csv(ofilepath, index=False)
df = pd.read_csv(myfilepath, index_col='date', parse_dates=True)
df = df.groupby(level=0).apply(s).reset_index(0, drop=True).sort_index().reset_index()
df = df.drop_duplicates(keep='first')
df.fee = df.fee * 100
df.fee = df.fee.apply(round)
df.fee = df.fee / 100
df.to_csv(myfilepath, index=False)

request = Request('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=3000&sort=changepercent&asc=0&node=hs_a&symbol=&_s_r_a=page&page=0')
text = urlopen(request, timeout=10).read()

reg = re.compile(r'\,(.*?)\:')
text = reg.sub(r',"\1":', text.decode('gbk') if ct.PY3 else text)
text = text.replace('"{symbol', '{"symbol')
jstr = json.dumps(text, encoding='GBK')
js = json.loads(jstr)
df = pd.DataFrame(pd.read_json(js, dtype={'code': object}),
                  columns=DAY_TRADING_COLUMNS)
df = df.drop('symbol', axis=1)