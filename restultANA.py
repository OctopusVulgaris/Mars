# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import matplotlib.pyplot as plt

import logging

def sort(x):
    x = x.sort_values('totalcap', ascending=True)
    x = x.head(300)

    return x


def calc(x):
    x = x.sort_index()
    y = x.close
    x = x.reset_index()

    result = pd.DataFrame()
    # yesterday
    result.loc[:, 'pclose'] = x.index - 1


    result = result.set_index(y.index)
    return result

def updateTmpHDF():
    t1 = datetime.datetime.now()
    print 'reading...'
    df = pd.read_csv('d:\\HDF5_Data\\rlt.csv', index_col='StartDate', parse_dates=True)
    del df['CapRange']
    del df['ED']
    del df['PTVERY']
    del df['ERSTD']
    del df['MAXDU']
    del df['CapRangeStart']
    #del df['Unnamed: 22']
    df.reset_index(inplace=True)

    df.set_index(['StartDate', 'Span', 'Holds', 'PLTS'], drop=True, inplace=True)
    df.sort_index(inplace=True)
    df = df.loc(axis=0)[:,[90,180,270,360,720],:,:]
    rt = df.RT.groupby(level=[0,1]).describe()
    rt_mean = rt.loc(axis=0)[:,:,'mean']


    aa = pd.DataFrame()

    aa[90] = rt_mean.loc(axis=0)[:,90]
    aa[180] = rt_mean.loc(axis=0)[:,180].reindex(aa.index, method='backfill')
    aa[270] = rt_mean.loc(axis=0)[:,270].reindex(aa.index, method='backfill')
    aa[360] = rt_mean.loc(axis=0)[:,360].reindex(aa.index, method='backfill')
    aa[720] = rt_mean.loc(axis=0)[:,720].reindex(aa.index, method='backfill')

    #print rt_mean.loc(axis=0)[:,180].reindex(aa.index, method='backfill')

    aa.reset_index(level=[1,2], drop= True, inplace=True)
    #print aa

    aa.plot(kind='line', grid=True, logy=False)






updateTmpHDF()

plt.show()
