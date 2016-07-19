# -*- coding:utf-8 -*-
import pandas as pd


def getopensplit():
    all = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', columns=['open'], where='date > \'2006-1-1\'')
    all = all[all.open > 0]
    codelist = all.index.get_level_values(0).to_series().get_values()

    for code in codelist:
        onedaytick = pd.read_hdf('d:\\HDF5_Data\\tick\\tick_tbl_'+code)
        onedaytick['open'] = all.open
        r = pd.DataFrame()
        r['upperamo'] = onedaytick[onedaytick.price >= onedaytick.open].groupby(level=[0,1]).amount.sum()
        r['loweramo'] = onedaytick[onedaytick.price <= onedaytick.open].groupby(level=[0,1]).amount.sum()
        r.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', append=True, complib='blosc', mode='a')


getopensplit()