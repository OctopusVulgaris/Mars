# -*- coding:utf-8 -*-

import sqlalchemy as sa
import datetime
import tushare as ts

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

succeed = False
while not succeed:
    try:
        t1 = datetime.datetime.now()
        succeed = True
    except Exception:
        continue

df = ts.get_today_all()
df['date'] = t1.date()

print datetime.datetime.now() - t1

succeed = False
while not succeed:
    try:
        df.to_sql('dayTrade', engine, if_exists='append', index=False, index_label=('code', 'date'), dtype={'date': sa.Date})
        succeed = True
    except Exception:
        continue


print datetime.datetime.now() - t1

