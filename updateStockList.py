# -*- coding:utf-8 -*-

import mydownloader
import threading
import dataloader
import sqlalchemy as sa

if __name__=="__main__":
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
    mydownloader.update_stock_basics(engine)











