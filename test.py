
import datetime as dt
import pandas as pd

c = pd.bdate_range(start='2017-12-31', end='2018-12-31')
holidays=[dt.date(2018,1,1),dt.date(2018,2,15),dt.date(2018,2,16),dt.date(2018,2,19),dt.date(2018,2,20),dt.date(2018,2,21),dt.date(2018,4,5),dt.date(2018,4,6),dt.date(2018,4,30),dt.date(2018,5,1),dt.date(2018,6,18),dt.date(2018,9,24),dt.date(2018,10,1),dt.date(2018,10,2),dt.date(2018,10,3),dt.date(2018,10,4),dt.date(2018,10,5)]
c = c.difference(holidays)
c = c[c.slice_indexer(end=dt.date.today()-dt.timedelta(days=1))]
lasttradeday = str(c[-1].date())
day = pd.read_hdf('d:/hdf5_data/dailydata.hdf', where='date=\'%s\'' % lasttradeday)