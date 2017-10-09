# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import os
import tushare as ts
import time
import subprocess as sp
import sys

def reconnect():
    sp.call('rasdial 宽带连接 /disconnect', stdout=sys.stdout)
    time.sleep(1)
    sp.call('rasdial 宽带连接 *63530620 040731', stdout=sys.stdout)
'''
def subx(x):
    return x[1] - x[0]

df = pd.read_csv('D:\\HDF5_Data\\fundmental\\601198.csv', encoding='gbk', index_col=u'报告日期', parse_dates=True).sort_index()
a = df.iloc[:,0].groupby(df.index.year).rolling(window=2).apply(subx).reset_index(level=0, drop=True).combine_first(df.iloc[:,0])
a.rolling(window=4).sum().combine_first(a)
'''
trade_type_dic = {
    '买盘' : 1,
    '卖盘' : -1,
    '中性盘' : 0
}

'''
all = all[all.open > 0]
grouped = all.groupby(['code', pd.Grouper(freq='1M', level=1)])
s =grouped.std()
s['opendelta'] = ((grouped.max()-grouped.min())/grouped.min()).open
v = grouped.vol.mean()
vp = v.groupby(level=0).pct_change()
s.vol = vp
s = s.dropna()
r = s[(s.open < 0.1) & (s.opendelta < 0.05) & (s.vol < -0.3)]

'''
def change_dic(x):
    if x == '--':
        return 0
    else:
        return x

all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2012-6-25\' and date < \'2012-6-28\' and code = \'000666\'')
all = all[all.open > 0]
all = all.reset_index(level=1)
a = all.index.drop_duplicates()
for code in a.values:
    print(code)
    path = 'D:\\HDF5_Data\\ticksn\\' + code
    if not os.path.exists(path):
        os.makedirs(path)

    datelist = all.loc[code:code].date
    if len(datelist) < 1:
        continue

    cachelist = os.listdir(path)
    if len(cachelist) > 0:
        datelist = datelist[datelist > cachelist[-1].rstrip('.csv')]

    for cur_day in datelist:
        succeeded = False
        retry = 0
        try:
            daypath = path + '\\' + str(cur_day.date()) + '.csv'

            #if os.path.exists(daypath):
            #    continue
            while not succeeded and (retry < 10):
                tick = ts.get_tick_data(code, date=str(cur_day.date()), retry_count=2, src='sn')
                if not tick.empty:
                    if tick.time[0] != 'alert("当天没有数据");':
                        tick['type'] = tick['type'].apply(lambda x: trade_type_dic[x])
                        tick['change'] = tick['change'].apply(change_dic)
                        #tick = tick.sort_values('time')
                        #tick.time = pd.to_timedelta(tick.time)
                        tick.change = tick.change.astype(float)
                        tick.to_csv(daypath, index=False)
                succeeded = True

        except Exception as e:
            retry += 1
            reconnect()



