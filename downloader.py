# -*- coding:utf-8 -*- 

import tushare as ts

instrument = ts.get_h_data('600705', '1990-01-01', '2100-01-01', 'qfq', False, 100, 1)
instrument.to_csv('d:\\a.csv')


