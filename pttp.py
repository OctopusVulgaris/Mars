# -*- coding:utf-8 -*-

import pandas as pd
import datetime as dt
import numpy as np
import ctypes as ct
import time
import talib as ta
import argparse
from utility import round_series, get_realtime_all_st, st_pattern
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import socket
import logging
import sys
import configparser
from shutil import copyfile


ashare_pattern = r'^0|^3|^6'


def prepare():
    t1 = time.clock()
    day = pd.read_hdf('d:/hdf5_data/dailydata.h5', columns=['open', 'high', 'low', 'close', 'hfqratio', 'stflag'], where='date > \'2007-1-1\'')
    day = day[day.open > 0]
    day['openorg'] = day.open
    day['open'] = day.open * day.hfqratio
    day['high'] = day.high * day.hfqratio
    day['low'] = day.low * day.hfqratio
    day['close'] = day.close * day.hfqratio
    day['ocmax'] = day[['open', 'close']].max(axis=1).groupby(level=0, group_keys=False).rolling(window=67).max()
    day['ocmin'] = day[['open', 'close']].min(axis=1).groupby(level=0, group_keys=False).rolling(window=67).min()
    day['ocrate'] = day.ocmax / day.ocmin

    fd = pd.read_hdf('d:/hdf5_data/fundamental.hdf')
    day['eps'] = fd['每股收益_调整后(元)']
    day['kama'] = day.groupby(level=0).apply(
        lambda x: pd.Series(ta.KAMA(x.close.values, timeperiod=22), x.index.get_level_values(1)))
    day['kamapct'] = day.kama.groupby(level=0).pct_change()+1
    day['kamaind'] = day.kamapct.groupby(level=0, group_keys=False).rolling(window=2).max()

    a = day.groupby(level=0).last()
    a['date'] = dt.datetime.today()
    a['idate'] = a.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    a = a.set_index([a.index, 'date'])
    a['open'] = 0
    a['high'] = 0
    a['low'] = 999999
    a['close'] = 0
    day = pd.concat([day, a])

    pday = day.groupby(level=0, group_keys=False).rolling(window=2).apply(lambda x: x[0])
    day['phigh'] = pday.high
    day['popen'] = pday.open
    day['plow'] = pday.low
    day['pclose'] = pday.close
    day['pkamaind'] = pday.kamaind
    day['highlimit'] = round_series(pday.close / day.hfqratio * 1.09)
    day['lowlimit'] = round_series(pday.close / day.hfqratio * 0.906)

    day['ppocrate'] = day.ocrate.groupby(level=0, group_keys=False).rolling(window=3).apply(lambda x: x[0])
    day['ppocmax'] = day.ocmax.groupby(level=0, group_keys=False).rolling(window=3).apply(lambda x: x[0])


    day = day.reset_index()
    day = day.set_index(['date', 'code'], drop=False)
    day.date = day.date.apply(lambda x: np.int64(time.mktime(x.timetuple())))
    day.code = day.code.apply(lambda x: np.int64(x))
    day = day.rename(columns={'date': 'idate', 'code': 'icode'})
    day = day.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values('ppocrate')).dropna()
    day.to_hdf('d:/hdf5_data/pttp.hdf', 'day', mode='w', format='t', complib='blosc')
    logging.info('all done...' + str(time.clock()-t1))

def initializeholding(type, prjname):
    BLSHdll = ct.cdll.LoadLibrary('D:/pttp.dll')

    BLSHdll.initialize.argtypes = [ct.c_void_p, ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_void_p,ct.POINTER(ct.c_double), ct.POINTER(ct.c_double), ct.c_void_p, ct.c_int, ct.c_double,ct.c_double, ct.c_int, ct.c_char_p]

    if type == 0:
        ll = 0
        cash = 300000
        total = 300000

        BLSHdll.initialize(ct.c_void_p(), ct.POINTER(ct.c_double)(), ct.POINTER(ct.c_double)(), ct.c_void_p(), ct.POINTER(ct.c_double)(), ct.POINTER(ct.c_double)(), ct.c_void_p(), ct.c_int(ll), ct.c_double(cash), ct.c_double(total), ct.c_int(type), ct.c_char_p(''.encode('ascii')))
        return

    initholding = pd.read_csv('d:/trade/%s/holding_week.csv' % (prjname), header=None, parse_dates=True, names=['date', 'code', 'buyprc','buyhfqratio', 'vol', 'daystosell', 'historyhigh', 'amount', 'cash', 'total'], dtype={'code': np.int64, 'buyprc': np.float64, 'buyhfqratio': np.float64, 'vol': np.int64, 'daystosell': np.int64, 'historyhigh': np.float64, 'amount': np.float64, 'cash': np.float64, 'total': np.float64}, index_col='date')

    if len(initholding) > 1:
        initholding = initholding.loc[initholding.index[-1]]

    ccode = initholding.code.get_values().ctypes.data_as(ct.c_void_p)
    cbuyprc = initholding.buyprc.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cbuyhfqratio = initholding.buyhfqratio.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cvol = initholding.vol.get_values().ctypes.data_as(ct.c_void_p)
    chistoryhigh = initholding.historyhigh.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    camount = initholding.amount.get_values().ctypes.data_as(ct.POINTER(ct.c_double))
    cdaystosell = initholding.daystosell.get_values().ctypes.data_as(ct.c_void_p)

    ll = len(initholding)
    cash = 200000
    total = 200000
    if ll > 0:
        cash = initholding.cash.get_values()[0]
        total = initholding.total.get_values()[0]

    BLSHdll.initialize(ccode, cbuyprc, cbuyhfqratio, cvol, chistoryhigh, camount, cdaystosell, int(ll), ct.c_double(cash), ct.c_double(total), int(type), ct.c_char_p(prjname.encode('ascii')))

def doProcessing(df, params):

    dll = ct.cdll.LoadLibrary('d:/pttp.dll')

    c_double_p = ct.POINTER(ct.c_double)

    # process
    dll.process.restype = ct.c_double
    dll.process.argtypes = [ct.c_void_p, ct.c_void_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, c_double_p, ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_int64, c_double_p]

    cdate = df.idate.get_values().ctypes.data_as(ct.c_void_p)
    ccode = df.icode.get_values().ctypes.data_as(ct.c_void_p)
    cp1 = df.eps.get_values().ctypes.data_as(c_double_p)
    cp2 = df.openorg.get_values().ctypes.data_as(c_double_p)
    cp3 = df.close.get_values().ctypes.data_as(c_double_p)
    cp4 = df.high.get_values().ctypes.data_as(c_double_p)
    cp5 = df.low.get_values().ctypes.data_as(c_double_p)
    cp6 = df.pkamaind.get_values().ctypes.data_as(c_double_p)
    cp7 = df.ppocrate.get_values().ctypes.data_as(c_double_p)
    cp8 = df.pclose.get_values().ctypes.data_as(c_double_p)
    cp9 = df.phigh.get_values().ctypes.data_as(c_double_p)
    cp10 = df.ppocmax.get_values().ctypes.data_as(c_double_p)
    cp11 = df.highlimit.get_values().ctypes.data_as(c_double_p)
    cp12 = df.lowlimit.get_values().ctypes.data_as(c_double_p)
    hfq = df.hfqratio.get_values().ctypes.data_as(c_double_p)
    cstflag = df.stflag.get_values().ctypes.data_as(ct.c_void_p)
    cactiveparam = params.get_values().ctypes.data_as(c_double_p)

    ret = dll.process(cdate, ccode, cp1, cp2, cp3, cp4, cp5, cp6, cp7, cp8, cp9, cp10, cp11, cp12, hfq, cstflag, cstflag, cstflag, len(df), cactiveparam)
    return ret

def regressionTest():
    logging.info('reading dayk tmp...' + str(datetime.datetime.now()))
    df = pd.read_hdf('d:/HDF5_Data/pttp.hdf', where='date > \'2008-1-1\'')



    t1 = time.clock()
    '''
    g_maxfallback = activeparam[0]
    epsflag = activeparam[1]
    ocrateflag = activeparam[2]
    buyselladj = activeparam[3]
    g_DELAYNOSELL = int64_t(activeparam[4])
    '''
    params = pd.Series([0.92, 1.2, 1.19, 0.999, 12, 1199116800])

    for g_maxfallback in [0.92,]:
        params[0] = g_maxfallback
        for epsflag in [1.2,]:
            params[1] = epsflag
            for ocrateflag in [1.19, ]:
                params[2] = ocrateflag
                for buyselladj in [0]:
                    params[3] = buyselladj
                    for g_DELAYNOSELL in [12,]:
                        params[4] = g_DELAYNOSELL
                        for startdate in [1191081600, 1192291200, 1192896000, 1193500800, 1194105600, 1194710400, 1195315200, 1195920000, 1196524800, 1197129600, 1197734400, 1198339200, 1198944000, 1199548800, 1200153600, 1200758400, 1201363200, 1201968000, 1202572800, 1203177600, 1203782400, 1204387200, 1204992000, 1205596800, 1206201600, 1206806400, 1207411200, 1208016000, 1208620800, 1209225600, 1209830400, 1210435200, 1211040000, 1211644800, 1212249600, 1212854400, 1213459200, 1214064000, 1214668800, 1215273600, 1215878400, 1216483200, 1217088000, 1217692800, 1218297600, 1218902400, 1219507200, 1220112000, 1220716800, 1221321600, 1221926400, 1222531200, 1223740800, 1224345600, 1224950400, 1225555200, 1226160000, 1226764800, 1227369600, 1227974400, 1228579200, 1229184000, 1229788800, 1230393600, 1230998400, 1231603200, 1232208000, 1232812800, 1234022400, 1234627200, 1235232000, 1235836800, 1236441600, 1237046400, 1237651200, 1238256000, 1238860800, 1239465600, 1240070400, 1240675200, 1241280000, 1241884800, 1242489600, 1243094400, 1243699200, 1244304000, 1244908800, 1245513600, 1246118400, 1246723200, 1247328000, 1247932800, 1248537600, 1249142400, 1249747200, 1250352000, 1250956800, 1251561600, 1252166400, 1252771200, 1253376000, 1253980800, 1254585600, 1255190400, 1255795200, 1256400000, 1257004800, 1257609600, 1258214400, 1258819200, 1259424000, 1260028800, 1260633600, 1261238400, 1261843200, 1262448000, 1263052800, 1263657600, 1264262400, 1264867200, 1265472000, 1266076800, 1267286400, 1267891200, 1268496000, 1269100800, 1269705600, 1270310400, 1270915200, 1271520000, 1272124800, 1272729600, 1273334400, 1273939200, 1274544000, 1275148800, 1275753600, 1276358400, 1276963200, 1277568000, 1278172800, 1278777600, 1279382400, 1279987200, 1280592000, 1281196800, 1281801600, 1282406400, 1283011200, 1283616000, 1284220800, 1284825600, 1285430400, 1286035200, 1286640000, 1287244800, 1287849600, 1288454400, 1289059200, 1289664000, 1290268800, 1290873600, 1291478400, 1292083200, 1292688000, 1293292800, 1293897600, 1294502400, 1295107200, 1295712000, 1296316800, 1296921600, 1297526400, 1298131200, 1298736000, 1299340800, 1299945600, 1300550400, 1301155200, 1301760000, 1302364800, 1302969600, 1303574400, 1304179200, 1304784000, 1305388800, 1305993600, 1306598400, 1307203200, 1307808000, 1308412800, 1309017600, 1309622400, 1310227200, 1310832000, 1311436800, 1312041600, 1312646400, 1313251200, 1313856000, 1314460800, 1315065600, 1315670400, 1316275200, 1316880000, 1317484800, 1318694400, 1319299200, 1319904000, 1320508800, 1321113600, 1321718400, 1322323200, 1322928000, 1323532800, 1324137600, 1324742400, 1325347200, 1325952000, 1326556800, 1327161600, 1328371200, 1328976000, 1329580800, 1330185600, 1330790400, 1331395200, 1332000000, 1332604800, 1333209600, 1333814400, 1334419200, 1335024000, 1335628800, 1336233600, 1336838400, 1337443200, 1338048000, 1338652800, 1339257600, 1339862400, 1340467200, 1341072000, 1341676800, 1342281600, 1342886400, 1343491200, 1344096000, 1344700800, 1345305600, 1345910400, 1346515200, 1347120000, 1347724800, 1348329600, 1348934400, 1350144000, 1350748800, 1351353600, 1351958400, 1352563200, 1353168000, 1353772800, 1354377600, 1354982400, 1355587200, 1356192000, 1356796800, 1357401600, 1358006400, 1358611200, 1359216000, 1359820800, 1360425600, 1361635200, 1362240000, 1362844800, 1363449600, 1364054400, 1364659200, 1365264000, 1365868800, 1366473600, 1367078400, 1367683200, 1368288000, 1368892800, 1369497600, 1370102400, 1370707200, 1371312000, 1371916800, 1372521600, 1373126400, 1373731200, 1374336000, 1374940800, 1375545600, 1376150400, 1376755200, 1377360000, 1377964800, 1378569600, 1379174400, 1379779200, 1380384000, 1380988800, 1381593600, 1382198400, 1382803200, 1383408000, 1384012800, 1384617600, 1385222400, 1385827200, 1386432000, 1387036800, 1387641600, 1388246400, 1388851200, 1389456000, 1390060800, 1390665600, 1391270400, 1391875200, 1392480000, 1393084800, 1393689600, 1394294400, 1394899200, 1395504000, 1396108800, 1396713600, 1397318400, 1397923200, 1398528000, 1399132800, 1399737600, 1400342400, 1400947200, 1401552000, 1402156800, 1402761600, 1403366400, 1403971200, 1404576000, 1405180800, 1405785600, 1406390400, 1406995200, 1407600000, 1408204800, 1408809600, 1409414400, 1410019200, 1410624000, 1411228800, 1411833600, 1412438400, 1413043200, 1413648000, 1414252800, 1414857600, 1415462400, 1416067200, 1416672000, 1417276800, 1417881600, 1418486400, 1419091200, 1419696000, 1420300800, 1420905600, 1421510400, 1422115200, 1422720000, 1423324800, 1423929600, 1424534400, 1425139200, 1425744000, 1426348800, 1426953600, 1427558400, 1428163200, 1428768000, 1429372800, 1429977600, 1430582400, 1431187200, 1431792000, 1432396800, 1433001600, 1433606400, 1434211200, 1434816000, 1435420800, 1436025600, 1436630400, 1437235200, 1437840000, 1438444800, 1439049600, 1439654400, 1440259200, 1440864000, 1441468800, 1442073600, 1442678400, 1443283200, 1443888000, 1444492800, 1445097600, 1445702400, 1446307200, 1446912000, 1447516800, 1448121600, 1448726400, 1449331200, 1449936000, 1450540800, 1451145600, 1451750400, 1452355200, 1452960000, 1453564800, 1454169600, 1454774400, 1455984000, 1456588800, 1457193600, 1457798400, 1458403200, 1459008000, 1459612800, 1460217600, 1460822400, 1461427200, 1462032000, 1462636800, 1463241600, 1463846400, 1464451200, 1465056000, 1465660800, 1466265600, 1466870400, 1467475200, 1468080000, 1468684800, 1469289600, 1469894400, 1470499200, 1471104000, 1471708800, 1472313600, 1472918400, 1473523200, 1474128000, 1474732800, 1475337600, 1476547200, 1477152000, 1477756800, 1478361600, 1478966400, 1479571200, 1480176000, 1480780800, 1481385600, 1481990400, 1482595200, 1483200000, 1483804800, 1484409600, 1485014400, 1485619200, 1486224000, 1486828800, 1487433600, 1488038400, 1488643200, 1489248000, 1489852800, 1490457600, 1491062400, 1491667200, 1492272000, 1492876800, 1493481600, 1494086400, 1494691200, 1495296000, 1495900800, 1496505600, 1497110400, 1497715200, 1498320000, 1498924800, 1499529600, 1500134400, 1500739200, 1501344000, 1501948800, 1502553600, 1503158400, 1503763200, 1504368000, 1504972800, 1505577600, 1506182400, 1506787200, 1507996800, 1508601600, 1509206400, 1509811200, 1510416000, 1511020800, 1511625600, 1512230400, 1512835200, 1513440000, 1514044800, 1514649600]:
                            params[5] = startdate

                            initializeholding(0, '')
                            ret = doProcessing(df, params)
                            hfile = 'h_' + '_'.join(str(x) for x in params) + '.csv'
                            tfile = 't_' + '_'.join(str(x) for x in params) + '.csv'
                            logging.info(hfile + str(ret))
                            copyfile('d:/tradelog/transaction_pttp_c.csv', 'd:/tradelog/pttp/' + tfile)
                            copyfile('d:/tradelog/holding_pttp_c.csv', 'd:/tradelog/pttp/' + hfile)
    logging.info('doProcessing...'+str(time.clock()-t1))
    logging.info('finished...' + str(ret))

def morningTrade():
    logging.info('retrieving today all...'+ str(dt.datetime.now()))
    realtime = pd.DataFrame()
    retry = 0
    get = False
    while not get and retry < 15:
        try:
            retry += 1
            # today = get_today_all()
            realtime = get_realtime_all_st()
            realtime = realtime.set_index('code')
            if realtime.index.is_unique and len(realtime[realtime.open > 0]) > 500:
                get = True
        except Exception:
            logging.error('retrying...')
            time.sleep(1)

    if realtime.sort_values('date').date.iloc[-1].date() < dt.date.today():
        logging.info('today ' + str(dt.date.today()) + ' is holiday, no trading...')
        return

    logging.info('reading temp file...' + str(dt.datetime.now()))
    df = pd.read_hdf('d:/HDF5_Data/pttp.hdf', 'day', where='date = \''+str(dt.date.today()) + '\'')

    realtime = realtime[realtime.pre_close > 0]
    df = df.reset_index(0)
    df.reindex(realtime.index, fill_value=0)

    df.date = dt.date.today()
    df.idate = np.int64(time.mktime(dt.date.today().timetuple()))
    df.open = realtime.open
    df.hfqratio = df.phfqratio * df.pclose / realtime.pre_close
    df.loc[realtime.name.str.contains(st_pattern), 'stflag'] = 1

    factor = df.phfqratio / df.hfqratio
    df.pclose = round_series(df.pclose * factor)
    df.plowlimit = round_series(df.plowlimit * factor)
    df.plow = round_series(df.plow * factor)
    df.phigh = round_series(df.phigh * factor)
    df.lowlimit = round_series(df.lowlimit * factor)
    df.highlimit = round_series(df.highlimit * factor)

    df = df[df.ptotalcap > 0]
    df = df[df.hfqratio > 1]
    df = df.sort_values('ptotalcap')
    df['upperamo'] = np.int64(0)
    df['loweramo'] = np.int64(0)

    df.to_hdf('d:/tradelog/today.hdf', 'day')

    #index = pd.read_hdf('d:\\HDF5_Data\\custom_totalcap_index.hdf', 'day')
    #index = index.fillna(0)
    #index = index.loc['2008-1-1':]
    #index.loc[datetime.date.today()] = index.loc['2050-1-1']

    logging.info('initializing holding...' + str(dt.datetime.now()))
    initializeholding(1)

    logging.info('doProcessing...' + str(datetime.datetime.now()))
    doProcessing(df, 1)

    logging.info('sending mail...' + str(datetime.datetime.now()))
    transactions = pd.read_csv('d:\\tradelog\\transaction_real_c.csv', header=None, parse_dates=True, names=['date', 'type', 'code', 'prc', 'vol', 'amount', 'fee', 'cash'], index_col='date')

    try:
        transactions.type.replace({0:'buy', 1:'sell out pool', 2:'sell open high', 3:'sell fallback', 4:'sell st flag'}, inplace=True)
        transactions = transactions.loc[datetime.date.today()]
    except KeyError:
        sendmail("no transaction today...")
    else:
        sendmail(transactions.to_string())
    logging.info('finished...' + str(datetime.datetime.now()))

def sendmail(log):
    config = configparser.ConfigParser()
    config.read('d:\\tradelog\\mail.ini')

    fromaddr = config.get('mail', 'from')
    toaddr = config.get('mail', 'to')
    password = config.get('mail', 'pw')
    msg = MIMEText(log, 'plain')
    msg['Subject'] = Header('BLSH@' + str(datetime.date.today())  + '_' + socket.gethostname())
    msg['From'] = fromaddr
    msg['To'] = toaddr

    try:
        sm = smtplib.SMTP_SSL('smtp.qq.com')
        sm.ehlo()
        sm.login(fromaddr, password)
        sm.sendmail(fromaddr, toaddr.split(','), msg.as_string())
        sm.quit()
    except Exception as e:
        logging.error(str(e))

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['prepare', 'regression', 'trade'], default='regression', help='one of \'prepare\', \'regression\', \'trade\'')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/blsh.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    if (type == 'regression'):
        regressionTest()
    elif (type == 'prepare'):
        prepare()
    elif (type == 'trade'):
        morningTrade()


