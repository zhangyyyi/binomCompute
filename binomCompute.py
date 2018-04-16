#!/usr/bin/env python
#coding=utf-8
"""class for data analysis of LP/GF/KW."""
"""AUTHOR: ZHANG YI"""



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt #%matplotlib inline
import sys
import scipy as sp
import sqlalchemy as sa
from sqlalchemy import create_engine
from scipy.stats import binom


engine = create_engine('oracle://BIP:fYzc8giLUlvj4dLaUvnm@192.168.1.208:1521/BIP')
time = ['TM', 'OM', 'EW']#周期：三个月 一个月 一周

def e_10(a):
    '''
    c = 0
    while a < 1:
        a = a*10
        c += 1
    :param a: 
    :return: 
    '''
    b = '%e'%a
    print(b[-3:])
    return b[-3:]


class TimeRangeSumm(object):

    def __init__(self, start, end):
            self.A = pd.Series(None, dtype = pd.DataFrame ,  index = pd.date_range(start, end,  freq = '3M') )
            self.B = pd.Series(None, dtype = pd.DataFrame ,  index = pd.date_range(start, end,  freq = 'M') )
            self.C = pd.Series(None, dtype = pd.DataFrame ,  index = pd.date_range(start, end,  freq = 'W') )

    def getDataFromDB(self, tablename):
        print("...start getting data from DB")
        for s in [self.A, self.B, self.C]:
            for item in range(len(s.index)-1):    #三个月源数据 , 每月源数据 半年周期 , 每周源数据  半年周期
                s[item] = pd.read_sql(sa.text( \
                "with   lp_summ as   \
                (  select rolename,:ss as mon,wintype, count(wintype) as qt from "+tablename+"  where workd >:start and   workd <=:end and wintype <> '0' group by rolename,wintype   order by rolename    ), \
                lp_wtype_no as  \
                (  Select rolename,:ss as mon, count(*) n From "+tablename+"  where workd >:start and   workd <=:end  group by rolename  )  \
                select a.*, b.n as amount from lp_summ a , lp_wtype_no b  where a.rolename=b.rolename and a.mon = b.mon "\
                \
                ), engine, params={'start': pd.to_datetime(s.index[item]), 'end': pd.to_datetime(s.index[item+1]), 'ss': pd.to_datetime(s.index[item])})
        print("...Have Gotten data from", tablename)
    def computePmf(self,npl):
        print("...start computing PMF")
        for lp in [self.A, self.B, self.C]:
            i = 0
            while(i < len(lp.index)-1):

                p = pd.Series(None, index=lp[i].index) #概率列初始化
                for col in ['pmf',  'sf',  'ql']:#['Phjths', 'Pths', 'Pst', 'Phl', 'Pth']:, 'sf'
                    lp[i][col] = p
                for m in range(len(lp[i].index)):
                    wintype = lp[i].loc[m,  'wintype']# 牌型编号 5, 6, 7, 8, 9 同花，葫芦，四条，同花顺，皇家同花顺
                    n = lp[i].loc[m,  'amount']
                    k = lp[i].loc[m,  'qt']#['hjths', 'ths', 'st', 'hl', 'th']''   #各牌型盘数
                    #self.A.iloc[i, start + 7] = comb(n, k, exact=True)*(npl[start]**k)*((1-npl[start])**(n-k))
                    if wintype != '0':
                        lp[i].loc[m, 'pmf'] = binom.pmf(k,  n,  npl[wintype],  loc=0) #一个月
                        lp[i].loc[m, 'sf'] = binom.sf(k, n, npl[wintype], loc=0)  # P[ X > k]
                        lp[i].loc[m, 'ql'] = ('%e'%lp[i].loc[m, 'pmf'])[-3:]#一个月

                    #lp[i].loc[m, 'isf'] = binom.isf(0.05,  n,  npl[wintype-5],  loc =0) #分位点
                    #lp[i].loc[m, 'interval'] = binom.interval(0.95,  n,  npl[wintype-5],  loc =0) #置信区间
                i = i + 1
                print("Have computed", i)

    def createOutputTable(self, tablehead):
        from pandas.io import sql
        i = 0
        while i < len(time):
            print("...start creating", tablehead + time[i])
            #table = str(tablehead + time[i])

            sql.execute(
            "create table "+tablehead + time[i]+" \
            (\
              rolename VARCHAR2(200),\
              mon      DATE,\
              wintype  VARCHAR2(40),\
              qt       NUMBER,\
              amount   NUMBER,\
              pmf      FLOAT,\
              sf       FLOAT,\
              ql       VARCHAR2(10)\
            )\
            tablespace BIP_TBS\
              pctfree 10\
              initrans 1\
              maxtrans 255\
              storage\
              (\
                initial 80K\
                next 1M\
                minextents 1\
                maxextents unlimited\
              )", engine)
            i = i + 1

    def writeDataToDB(self,  tablehead):
        arr = [self.A,  self.B,  self.C]
        i = 0
        while i < len(time):
            print("...start writing to", tablehead + time[i])
            for item in arr[i][:-1]:
                item.to_sql(tablehead + time[i],  engine,  if_exists='append', index=False)
            i = i + 1

        print("ALL Done!")


class TimeRangeSumm4g(TimeRangeSumm):
    def getDataFromDB(self):
        print("...start getting data from DB")
        for s in [self.A, self.B, self.C]:
            for item in range(len(s.index) - 1):  # 三个月源数据 , 每月源数据 半年周期 , 每周源数据  半年周期
                s[item] = pd.read_sql(sa.text( \
                    "with   gf_summ as   \
                    (  select rolename,:ss as mon,cardtype as wintype, count(cardtype) as qt from V_texas_gf_detail  where workd >:start and   workd <=:end and comnum <> 0 group by rolename,cardtype  order by rolename    ), \
                        gf_wtype_no as  \
                        (  Select rolename,:ss as mon, count(*) n From V_texas_gf_detail  where workd >:start and   workd <=:end  group by rolename  )  \
                        select a.*, b.n as amount from gf_summ a , gf_wtype_no b  where a.rolename=b.rolename and a.mon = b.mon " \
 \
                    ), engine, params={'start': pd.to_datetime(s.index[item]), 'end': pd.to_datetime(s.index[item + 1]),
                                       'ss': pd.to_datetime(s.index[item])})
        print("...Have Gotten data from V_texas_gf_detail")#写死


class TimeRangeSumm4k(TimeRangeSumm):
    def getDataFromDB(self):
        print("...start getting data from DB")
        for s in [self.A, self.B, self.C]:
            for item in range(len(s.index) - 1):  # 三个月源数据 , 每月源数据 半年周期 , 每周源数据  半年周期
                s[item] = pd.read_sql(sa.text( \
"with \
kw_summ as \
(\
      select rolename,:ss as mon,'han' as wintype,count(*)  as qt          \
         from texas.v_texas_kingswar_detail \
        where workd > :start and   workd <= :end \
        and gameresult = '汉胜' and han <>0 \
         group by rolename\
             \
union all \
\
      (\
          select rolename,:ss as mon,'chu' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and gameresult = '楚胜' and chu <>0 \
             group by rolename \
\
      )\
      \
union all \
\
      (\
          select rolename,:ss as mon,'ping' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and gameresult = '平' and ping<> 0 \
             group by rolename \
\
      )\
      \
union all \
\
      (\
          select rolename,:ss as mon,'highandone' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and winusercardtype <= 1 and highandone <>0 \
             group by rolename\
\
      )\
      \
union all \
\
      (\
          select rolename,:ss as mon,'twopairs' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and winusercardtype =2  and twopairs <>0 \
             group by rolename \
\
      )\
      \
union all \
\
      (\
          select rolename,:ss as mon,'threestraiflu' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
           where workd > :start and   workd <= :end \
            and winusercardtype >2 and winusercardtype <=5 and threestraiflu <>0 \
             group by rolename \
\
      )\
\
union all \
\
      (\
          select rolename,:ss as mon,'fullhouse' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and winusercardtype =6 and fullhouse <> 0 \
             group by rolename\
\
      )\
      \
union all \
\
      (\
          select rolename,:ss as mon,'fourstraiandroyalflu' as wintype,count(*)  as qt          \
             from texas.v_texas_kingswar_detail \
            where workd > :start and   workd <= :end \
            and winusercardtype >6 and fourstraiandroyalflu <> 0 \
             group by rolename\
\
      )      \
),\
kw_wtype_no as\
(\
Select rolename,:ss as mon,count(*) n From texas.v_texas_kingswar_detail \
where workd > :start and   workd <= :end \
group by rolename \
)\
select a.*,b.n as amount from kw_summ a ,kw_wtype_no b \
where a.rolename=b.rolename and a.mon = b.mon \
order by a.rolename\
"
                    ), engine, params={'start': pd.to_datetime(s.index[item]), 'end': pd.to_datetime(s.index[item + 1]),
                                       'ss': pd.to_datetime(s.index[item])})
        print("...Have Gotten data from texas.v_texas_kingswar_detail")#写死



