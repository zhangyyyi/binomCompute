#!/usr/bin/env python
#coding=utf-8
"""project for data analysis of LP/GF/KW."""
"""AUTHOR: ZHANG YI"""

import time
from binomCompute import TimeRangeSumm
from binomCompute import TimeRangeSumm4g
from binomCompute import TimeRangeSumm4k


npl = {'5': 0.028289540, '6': 0.024520446, '7': 0.001440576, '8': 0.000264656, '9': 0.000030782}  # 彩金各牌型自然概率
npg = {'red': 0.117648, 'black': 0.117648, 'flush': 0.051764, 'straight': 0.034751, 'three_of_a_kind': 0.002353,
       'straight_flush': 0.002172}  # 猜翻牌
npk = {'chu': 0.47965, 'han': 0.47965, 'ping': 0.040675, 'fourstraiandroyalflu': 0.003725, 'fullhouse': 0.047025,
       'threestraiflu': 0.204556, 'twopairs': 0.314167, 'highandone': 0.4305}  # 楚汉
def main(startdate,  enddate):



    if len(startdate) != 8 or len(enddate) != 8:
        print("Error: input start time like 'yyyymmdd'!")
    else:
        #print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        start = time.time()
        lp = TimeRangeSumm(startdate,  enddate)
        lp.getDataFromDB('v_texas_lpcharge_tr')
        lp.computePmf(npl)
        lp.createOutputTable('Y_TEMP_LP_')
        lp.writeDataToDB('Y_TEMP_LP_')
        t1 = time.time()-start
        print("LP cost time:", t1)

        
       #gf --change the method of getDataFromDB

        gf = TimeRangeSumm4g(startdate, enddate)
        gf.getDataFromDB()
        gf.computePmf(npg)
        gf.createOutputTable('Y_TEMP_GF_')
        gf.writeDataToDB('Y_TEMP_GF_')#写入表需先建表
        t2 = time.time() - t1
        print("GF cost time:", t2)

'''
        #start = time.time()
        kw = TimeRangeSumm4k(startdate, enddate)# 重载类
        kw.getDataFromDB()
        kw.computePmf(npk) # 全局概率
        kw.createOutputTable('Y_TEMP_KW_')
        kw.writeDataToDB('Y_TEMP_KW_')  # 写入表需先建表
        t3 = time.time() - t2
        print("KW cost time:", t3)
'''

if __name__ == "__main__":
    main('20170531',  '20171201')