# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import Queue
import time
import logging
import sys
import talib as ta


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/test.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:/HDF5_Data/dailydata.h5', 'dayk', columns=['close', 'hfqratio'], where='date > \'2006-1-1\'')

    ft = pd.read_hdf('d:/HDF5_Data/filteredtick.hdf')
    ft['rate'] = ft.small / (ft.small + ft.medium + ft.large + ft.huge)
    ft['rate'] = ta.MA(ft.rate.values, timeperiod=3)

    ft['close'] = all.close * all.hfqratio
    ft.fillna(0, inplace=True)

    ft = ft[['rate', 'close']]

    #ft['ma7'] = ft.groupby(level=0).apply(lambda x: pd.Series(ta.MA(x.rate.values, timeperiod=7), x.index.get_level_values(1))).fillna(0)

    nft = ft.groupby(level=0).apply(lambda x: (x - x.mean()) / x.std())
    nft.to_hdf('d:/hdf5_data/normalizedSTR.hdf', 'day', format='f', mode='w', complib='blosc')

    nft['ma10'] = ta.MA(nft.rate.values, timeperiod=10)
    nft['ma20'] = ta.MA(nft.rate.values, timeperiod=20)
    nft['ma30'] = ta.MA(nft.rate.values, timeperiod=30)
    nft['ma60'] = ta.MA(nft.rate.values, timeperiod=60)
    nft['ma90'] = ta.MA(nft.rate.values, timeperiod=90)
    nft['ma120'] = ta.MA(nft.rate.values, timeperiod=120)
    nft['ma240'] = ta.MA(nft.rate.values, timeperiod=240)

    correls = ft.groupby(level=0).rolling(window=120).corr()

    dfs = []
    df = pd.DataFrame()
    for x in correls.values:
        dfs.append(x.loc[:, 'rate', 'close'])
    df['cor'] = pd.concat(dfs)
    df['ma5'] = ta.MA(ft.rate.values, timeperiod=5)
    df['ma10'] = ta.MA(ft.rate.values, timeperiod=10)
    df['ma20'] = ta.MA(ft.rate.values, timeperiod=20)
    df['ma30'] = ta.MA(ft.rate.values, timeperiod=30)
    df['ma60'] = ta.MA(ft.rate.values, timeperiod=60)
    df['ma90'] = ta.MA(ft.rate.values, timeperiod=90)
    df['ma120'] = ta.MA(ft.rate.values, timeperiod=120)
    df['ma240'] = ta.MA(ft.rate.values, timeperiod=240)
    df['min10'] = ta.MIN(ft.rate.values, timeperiod=10)
    df['min20'] = ta.MIN(ft.rate.values, timeperiod=20)
    df['min30'] = ta.MIN(ft.rate.values, timeperiod=30)
    df['min60'] = ta.MIN(ft.rate.values, timeperiod=60)
    df['min90'] = ta.MIN(ft.rate.values, timeperiod=90)
    df['min120'] = ta.MIN(ft.rate.values, timeperiod=120)
    df['min240'] = ta.MIN(ft.rate.values, timeperiod=240)
    df['max10'] = ta.MAX(ft.rate.values, timeperiod=10)
    df['max20'] = ta.MAX(ft.rate.values, timeperiod=20)
    df['max30'] = ta.MAX(ft.rate.values, timeperiod=30)
    df['max60'] = ta.MAX(ft.rate.values, timeperiod=60)
    df['max90'] = ta.MAX(ft.rate.values, timeperiod=90)
    df['max120'] = ta.MAX(ft.rate.values, timeperiod=120)
    df['max240'] = ta.MAX(ft.rate.values, timeperiod=240)
    df.fillna(0, inplace=True)
    r = df[(df.cor > 0.8) & (df.ma60 > df.ma10) & (df.ma5 > 0.4)]