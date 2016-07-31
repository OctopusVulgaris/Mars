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
    ft['close'] = all.close * all.hfqratio
    ft.fillna(0, inplace=True)

    #ft['ma7'] = ft.groupby(level=0).apply(lambda x: pd.Series(ta.MA(x.rate.values, timeperiod=7), x.index.get_level_values(1))).fillna(0)

    #nft = ft[['close', 'ma7']].groupby(level=0).apply(lambda x: (x - x.mean()) / x.std())
    #nft.to_hdf('d:/hdf5_data/normalizedSTR.hdf', 'day', format='f', mode='w', complib='blosc')

    g5d = ft.groupby(level=0).rolling(window=5)
    g5d.close.corr(g5d.rate)
