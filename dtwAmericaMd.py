# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 16:05:40 2016

@author: dingchaoqun
"""
from bs4 import BeautifulSoup
import urllib.request
from datetime import *
import time , os,requests
import numpy as np
#import pprint
import tables
import tstables
import pandas as pd
class MdValues(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    open = tables.Float64Col(pos=1)
    high = tables.Float64Col(pos=2)
    low = tables.Float64Col(pos=3)
    close = tables.Float64Col(pos=4)
    vol = tables.Float64Col(pos=5)
   
def getMd(stockcode):
    json_url = "http://chartapi.finance.yahoo.com/instrument/1.0/%s/chartdata;type=quote;range=1d/csv"%stockcode
    data = urllib.request.urlopen(json_url).read()
    datastr = data.decode('ascii')
    datastrs=datastr.split('\n')
    words=  list(filter(lambda x:x.find(':')<0 and len(x)>0,datastrs))
   
    dtime = []
    openp = []
    closep = []
    highp = []
    lowp = []
    vol = []
   
    for x in words:
        xs= x.split(',')
        print("%s %s %s  %s %s"%(xs[0],xs[1],xs[2],xs[3],xs[4]))
        dtime.append(datetime.fromtimestamp(float(xs[0])))
        openp.append (float(xs[1]))
        closep.append(float(xs[2]))
        highp.append(float(xs[3]))
        lowp.append(float(xs[4]))
        vol.append(float(xs[5]))
   
    md = pd.DataFrame()
    md['time'] = dtime
    md['close'] = openp
    md['high'] = closep
    md['low'] = highp
    md['open'] = lowp
    md['vol'] = vol
    md=md.set_index('time')
    md.sort_index(inplace=True)
    md.drop_duplicates(keep='last', inplace=True)
   
    #x = pd.DataFrame(datastrs)
    return md
   

f =  tstables.tables.open_file('bpiAB12.h5','a',complevel=9, complib='zlib')

def updatestock():
    global bx    
    store = pd.HDFStore('store.h5')
    if store.get_node('store') :
            bx=store['store']
            print ('ttt')


stockcode1= 'WLL'
for stockcode in [stockcode1]:
    print(stockcode)
    md = getMd(stockcode)
    #rootNodes = f.list_nodes('/')
    if f.__contains__('/'+stockcode):
        print("node exist")
        #bb= tstables.get_timeseries(f.root,'test')
        stockcodenode =  f.get_node('/'+stockcode)
        bb=tstables.TsTable(f,stockcodenode,MdValues)
        maxdt = bb.max_dt()
        addmd = md[md.index>maxdt]
        bb.append(addmd)
    else:
        print("node not")
        bb=f.create_ts('/',stockcode,MdValues)
        bb.append(md)
   
stockcode =stockcode1
cmd = "dayparser.exe 1m 74#%s.lc1 >74#%s.lc1.csv"%(stockcode,stockcode)
os.system(cmd)  

#class MdValues(tables.IsDescription):
#    timestamp = tables.Int64Col(pos=0)
#    open = tables.Float64Col(pos=1)
#    high = tables.Float64Col(pos=2)
#    low = tables.Float64Col(pos=3)
#    close = tables.Float64Col(pos=4)
#    vol = tables.Int64Col(pos=5)

fb = tstables.tables.open_file('ONEMINUTES_MDZ.h5','a',complevel=9, complib='zlib')

md = pd.read_csv('74#%s.lc1.csv'%(stockcode),index_col=0,\
    names=['no','date','open','high','low','close','bpi','vol'],\
    usecols=['date','open','high','low','close','vol'], \
     dtype={'open': np.float64,'high': np.float64,'low': np.float64,\
     'close': np.float64,'vol': np.float64},parse_dates=True)
md.sort_index(inplace=True)
md.drop_duplicates()

#stockcode ='WLL'
if fb.__contains__('/'+stockcode):
    node = fb.get_node('/'+stockcode)
    ts = tstables.TsTable(None,node,MdValues)
    addmd = md[md.index>ts.max_dt()]
    ts.append(addmd)
else:
    print("noe")
    ts=  fb.create_ts('/',stockcode,MdValues)
    ts.append(md)


from numpy import linalg as LA
from dtw import dtw
import matplotlib.cm as cm
from sklearn import preprocessing
from sklearn.metrics.pairwise import manhattan_distances
def timestamp2datetime(timestamp, convert_to_local=False):
  ''' Converts UNIX timestamp to a datetime object. '''
  if isinstance(timestamp, (int, float)):
    dt = datetime.datetime.utcfromtimestamp(timestamp)
    if convert_to_local: # 是否转化为本地时间
      dt = dt + datetime.timedelta(hours=8) # 中国默认时区
    return dt
  return timestamp
td=datetime.today()
read_start_dt = datetime(td.year,td.month,td.day-1,20)
read_end_dt = datetime(td.year,td.month,td.day,20)
todata = bb.read_range(read_start_dt,read_end_dt)
row3 = todata.iloc[0:50][['close']]
row4 = todata.iloc[50:100][['close']]

rows = bb.read_range(read_start_dt,read_end_dt)

datacalc = rows[['close']]
datacalc2 = datacalc[datacalc.index.hour>20]
datacalc2.drop_duplicates()
#
converted = datacalc2.resample('D')
dates = pd.date_range('20150105 20:00:00', '20151231 20:00:00')

dists = []
costs = []
dts= []
this1h = []
next1h = []
today1h = []
for i in range(len(dates)-1):
    print(i)
    fromdata = ts.read_range(timestamp2datetime(dates[i]),timestamp2datetime(dates[i+1]))
    fromdata = fromdata[fromdata['vol']>0]
    if len(fromdata)>=100:
        rows1 = fromdata.iloc[0:50][['close']]
        rows2 = fromdata.iloc[50:100][['close']]
        x= preprocessing.scale(rows1['close'])#.reshape(-1,1)
        y= preprocessing.scale(row3['close'])#.reshape(-1,1)
        dist, cost, acc,path = dtw(x, y,dist = manhattan_distances)
        dts.append(dates[i])        
        dists.append(dist)
        costs.append(cost)
        this1h.append(rows1['close'])
        next1h.append(rows2['close'])
        today1h.append(row3['close'])
df = pd.DataFrame()
df['date']=dts
df['cost']=costs
df['dist']=dists
df['this1h']=this1h
df['next1h']=next1h
df['today1h']=today1h

df.sort_values('dist',inplace=True)
df=df.reset_index()


import matplotlib.pyplot as plt
for i in range(20):
    plt.figure(i)
    #df['today1h'][i].plot()
    if len(df['this1h'][i])>0:
        df['this1h'][i].plot()
    if len(df['next1h'][i])>0:
        df['next1h'][i].plot()
fb.close()

#row4 = bb.read_range('20161028 20:00:00','20161029 20:00:00').iloc[0:45][['close']]