
l = getcodelist(True)
url = 'http://qt.gtimg.cn/q=' + ','.join(l[:50].code.apply(lambda x: 'sh'+ x if x.startswith('6') else 'sz'+ x))



a = pd.read_table(url, encoding='gbk', delimiter='~', names=['','name','code','close','prevclose','open','volume','invol','outvol','buy1','bsize1','buy2','bsize2','buy3','bsize3','buy4','bsize4','buy5','bsize5','sell1','ssize1','sell2','ssize2','sell3','ssize3','sell4','ssize4','sell5','ssize5','trades','date','netchng','pctchng','high','low','trade','vol','amo','turnoverrate','pe','a','h','l','m','tradeablecap','totalcap','pb','highlimit','lowlimit','b','c','d','e','f'],usecols=['name','code','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','date','vol','amo','totalcap','tradeablecap'], lineterminator='"', engine='c', dtype={'name':str, 'code':str,'close':float,'prevclose':float,'open':float,'high':float,'low':float,'vol':float,'amo':float,'tradeablecap':float,'totalcap':float,'turnoverrate':float,'netchng':float,'pctchng':float}, parse_dates=['date']).dropna()

a = a[['code', 'date', 'name', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng', 'turnoverrate', 'vol', 'amo', 'totalcap', 'tradeablecap']]
a['date'] = a.date.apply(lambda x: x.date())
a = a.set_index(['code','date'])
a['vol'] = a.vol.astype(np.int64)
a['amo'] = a.amo * 10000
a['totalcap'] = a.totalcap * 100000000
a['tradeablecap'] = a.tradeablecap * 100000000
a['stflag'] = 0
a.loc[a.name.str.contains(st_pattern), 'stflag'] = 1
a['hfqratio'] = 1.0