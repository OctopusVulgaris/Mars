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
from utility import round_series, getcodelist, getindexlist, reconnect, st_pattern
import argparse
import talib as ta

day = pd.read_hdf('d:/hdf5_data/dailydata.h5', columns=['open', 'high', 'low', 'close', 'hfqratio', 'name'], where='date > \'2007-1-1\'')
day = day[day.open > 0]
day['open'] = day.open * day.hfqratio
day['high'] = day.high * day.hfqratio
day['low'] = day.low * day.hfqratio
day['close'] = day.close * day.hfqratio
day['ocmax'] = day[['open', 'close']].max(axis=1).groupby(level=0, group_keys=False).rolling(window=67).max()
day['ocmin'] = day[['open', 'close']].min(axis=1).groupby(level=0, group_keys=False).rolling(window=67).min()
day['ocrate'] = day.ocmax / day.ocmin

fd = pd.read_hdf('d:/hdf5_data/fundamental.hdf')
day['eps'] = fd['每股收益_调整后(元)']
day['kama'] = day.groupby(level=0).apply(lambda x: pd.Series(ta.KAMA(x.close.values, timeperiod=22), x.index.get_level_values(1)))
day['kamapct'] = day.kama.groupby(level=0).pct_change()
day['kamaind'] = day.kamapct.groupby(level=0).rolling(window=2).max()

pday = day.groupby(level=0, group_keys=False).rolling(window=2).apply(lambda x: x[0])
day['phigh'] = pday.high
day['popen'] = pday.open
day['plow'] = pday.low
day['pclose'] = pday.close
day['highlimit'] = day.pclose * 1.1
day['lowlimit'] = day.pclose * 0.9
day['stflag'] = 0
day.loc[day.name.str.contains(st_pattern), 'stflag'] = 1
day = day.swaplevel(0,1)
day = day.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values('ocrate')).dropna()
day.to_hdf('d:/hdf5_data/PTTP.hdf', 'day', mode='w', format='t', complib='blosc')