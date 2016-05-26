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
    rt = df.RT.groupby(level=[1,2,3]).describe()
    rt_mean =  rt.loc(axis=0)[:,:,:,'mean']
    rt_mean.plot()
    print rt_mean.index.names
    print pd.value_counts(rt_mean.index.get_level_values(0))





updateTmpHDF()

plt.show()
