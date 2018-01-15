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
import talib as ta

day = pd.read_hdf('d:/hdf5_data/dailydata.h5')
day = day[day.open > 0]
day['open'] = day.open * day.hfqratio
day['close'] = day.close * day.hfqratio
day['ocmax'] = day[['open','close']].max(axis=1).groupby(level=0,group_keys=False).rolling(window=67).max()
day['ocmin'] = day[['open','close']].min(axis=1).groupby(level=0,group_keys=False).rolling(window=67).min()
day['ocrate'] = day.ocmax / day.ocmin

fd = pd.read_hdf('d:/hdf5_data/fundamental.hdf')
day['eps'] = fd['每股收益_调整后(元)']
ta.KAMA()
