# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime

t1 = datetime.datetime.now()
delta = datetime.timedelta(days=1)
cur_day = datetime.datetime.strptime('2005-01-01', '%Y-%m-%d')
next_day = cur_day + delta
last_day = datetime.datetime.strptime('2006-01-01', '%Y-%m-%d')

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
sql = "SELECT * FROM tick_tbl_000001 where time > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and time < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
one_year_tick = pd.read_sql(sql, engine, index_col='time', parse_dates={'time' : datetime})


while cur_day != last_day:

    one_day_tick = one_year_tick[cur_day.strftime('%Y-%m-%d'):next_day.strftime('%Y-%m-%d')]
    if not one_day_tick.empty:
        print one_day_tick


    cur_day = cur_day + delta
    next_day = next_day + delta

t1 = datetime.datetime.now()-t1
print t1