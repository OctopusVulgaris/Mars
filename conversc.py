# -*- coding:utf-8 -*-
import pandas as pd
import datetime
import logging
import numpy as np
import re

logging.basicConfig(filename='test_log.txt', level=logging.DEBUG)
from sqlalchemy import Date, text, DateTime, Integer

datepattern = r'\[(\d*\-\d*\-\d*)\]'
buypattern = r'Buy (\d{6}).* on (.*\d), at (.*) vol (.*) fee (.*) avaCap.*'
sellpattern = r'Sell (\d{6}).* on (.*), at (.*) ER.* amount (.*) profit.* fee (.*) Avacap.*'
ifilepath = 'd:\\sc.txt'
ofilepath = 'd:\\sc.csv'
myfilepath = 'd:\\tradelog\\transaction_c.csv'
omyfilepath = 'd:\\transaction_c.csv'
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
            #date = datetime.datetime.strptime(m.groups()[1], '%Y-%m-%d')
            #sdate = date.strftime('%m/%d/%Y')
            oline = m.groups()[1] + ',buy,' + m.groups()[0] + '\n'
            olines.append(oline)
            line = ifile.readline()
            continue
        m = re.search(sellpattern, line)
        if m:
            #date = datetime.datetime.strptime(m.groups()[1], '%Y-%m-%d')
            #sdate = date.strftime('%m/%d/%Y')
            oline = m.groups()[1] + ',sell,' + m.groups()[0] + '\n'
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
df.to_csv(ofilepath, index=False, columns=['date', 'type', 'code'])

df = pd.read_csv(myfilepath, header=None, parse_dates=True, names=['date', 'type', 'code', 'prc', 'amount', 'fee', 'cash'], index_col='date')

df.type = df.type.replace({0:'buy', 1:'sell', 2:'sell', 3:'sell', 4:'sell'})
df = df.groupby(level=0).apply(s).reset_index(0, drop=True).sort_index().reset_index()
del df['prc']
del df['amount']
del df['fee']
del df['cash']
df = df.drop_duplicates(keep='first')
df.to_csv(omyfilepath, index=False, columns=['date', 'type', 'code'])



