# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 15:37:05 2016

@author: dingchaoqun
"""

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


stockcode =stockcode1
cmd = "dayparser.exe 1m 74#%s.lc1 >74#%s.lc1.csv"%(stockcode,stockcode)
os.system(cmd)  

fb = tstables.tables.open_file('ONEMINUTES_MDZ1.h5','a',complevel=9, complib='zlib')

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


