# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 19:45:42 2021

@author: Satish
"""

import pandas as pd
import datetime
import time
import numpy as np


def jez(x):
    if (x[-1]==0 and x[0]==1):
        return (True)
    else:
        return (False)


    # return (x[0] != x[-1])

def change(x):
    if x=='visitPage':
        z=int(1)
    else:
        z=int(0)
    return(z)

dtset=pd.read_csv('events_log.csv')
dtset['date'] = pd.to_datetime(dtset['timestamp'], format='%Y%m%d%H%M%S').dt.date
dtset['time'] = pd.to_datetime(dtset['timestamp'], format='%Y%m%d%H%M%S').dt.time

dtset=dtset.sort_values('timestamp')
z=dtset[dtset['action']=='searchResultPage'].groupby(['date'],as_index='False').action.count()
y=dtset[dtset['action']=='visitPage'].groupby(['date'],as_index='False').action.count()
Ans1A=pd.concat([z,y],axis=1,keys=['No of SRP','No of Interest'])
t=dtset[dtset['action']!='checkin']
t1=t.apply(lambda x: change(x['action']), axis=1)
t['Status']=t1
t1=t.groupby(by=['date','session_id']).Status.rolling(2).apply(lambda x: jez(x), raw=True).fillna(0)
t2=t1.groupby(['date']).sum()
Ans1A['Real no of Interests']=t2.values
Ans1A['CThru rate']=Ans1A['Real no of Interests']/Ans1A['No of SRP']


z=dtset[dtset['action']=='searchResultPage'].groupby(by=['group']).action.count()
y=dtset[dtset['action']=='visitPage'].groupby(by=['group']).action.count()
Ans1B=pd.concat([z,y],axis=1,keys=['No of SRP','No of Interest'])
t=dtset[dtset['action']!='checkin']
t1=t.apply(lambda x: change(x['action']), axis=1)
t['Status']=t1
t1=t.groupby(by=['group','session_id']).Status.rolling(2).apply(lambda x: jez(x), raw=True).fillna(0)
t2=t1.groupby(['group']).sum()
Ans1B['Real no of Interests']=t2.values
Ans1B['CThru rate']=Ans1B['Real no of Interests']/Ans1B['No of SRP']

z=dtset[dtset['action']=='searchResultPage'].groupby(by=['date']).action.count()
y=dtset[dtset['n_results']==0].groupby(by=['date']).action.count()
Ans3A=pd.concat([z,y],axis=1,keys=['No of SRP','No of zero results'])
Ans3A['Zero rate']=Ans3A['No of zero results']/Ans3A['No of SRP']

z=dtset[dtset['action']=='searchResultPage'].groupby(by=['group']).action.count()
y=dtset[dtset['n_results']==0].groupby(by=['group']).action.count()
Ans3B=pd.concat([z,y],axis=1,keys=['No of SRP','No of zero results'])
Ans3B['Zero rate']=Ans3B['No of zero results']/Ans3B['No of SRP']

dtset=dtset.sort_values(by=['session_id','timestamp'])
z=dtset[dtset['action']=='visitPage'].groupby(by=['date','session_id']).result_position.first()
z=pd.DataFrame(z)
z=pd.DataFrame(z.values,index=z.index.droplevel(1),columns=['result_position'])
Ans2A=z.groupby(by=['date']).result_position.agg(pd.Series.mode)

z=dtset[dtset['action']=='visitPage'].groupby(by=['group','session_id']).result_position.first()
z=pd.DataFrame(z)
z=pd.DataFrame(z.values,index=z.index.droplevel(1),columns=['result_position'])
Ans2B=z.groupby(by=['group']).result_position.agg(pd.Series.mode)

dtset=dtset.sort_values(by=['session_id','timestamp'])
z=dtset.groupby(by='session_id',as_index='False').timestamp.min()
y=dtset.groupby(by='session_id',as_index='False').timestamp.max()
z1=pd.concat([z,y],axis=1,keys=['starttime','stoptime'])
z1['starttime']=pd.to_datetime(z1['starttime'], format='%Y%m%d%H%M%S')
z1['stoptime']=pd.to_datetime(z1['stoptime'], format='%Y%m%d%H%M%S')
z1['timeelapsed']=(z1['stoptime']-z1['starttime'])/pd.Timedelta(minutes=1)
y=dtset[dtset['action']=='searchResultPage'].groupby(by='session_id',as_index='False').action.count()
z1=pd.concat([z1,y],axis=1)
z1['action']=z1['action'].fillna(0)
y=dtset[dtset['action']=='searchResultPage'].groupby(by='session_id',as_index='False').n_results.sum()
z1=pd.concat([z1,y],axis=1)
z1['n_results']=z1['n_results'].fillna(0)
y=dtset.groupby(by='session_id',as_index='False').group.first()
z1=pd.concat([z1,y],axis=1)
print(f"{z1['timeelapsed'].max()}")

