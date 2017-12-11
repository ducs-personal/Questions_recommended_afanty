# -*-coding:utf8-*-
import os
import sys
import csv
import logging
import pandas as pd
import numpy as np
import json
from multiprocessing import Pool
from data_mining.FPGrowth import fpGrowth
from lib.util import getProvinceSet,mkdir

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
ANALY = _DIR + '/new_database/Analy/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
FPGROWTH_PATH = ANALY + 'FPGrowth_output/'

prov_subj_file = 'prov_subject_{}_{}_{}.txt'
fpgth_output_file = 'fp_growth_{}_{}_{}.txt'


#修饰数据
def dealJsonData(data_json):
    for i in range(len(data_json)):
        data_json[i] = list(set(eval(data_json[i].replace('\n',''))[1:]))

    return data_json


def getfreqitem(dataSet, minS):
    freqItems = []
    freqlist = fpGrowth(dataSet, minSup=minS)

    for i in freqlist:
        if len(i) >= 2:
            freqItems.append(i)

    return freqItems


def bigFreqItems(dataSet, minS_k):
    door_len = 0
    minSup = 150

    while door_len < 5000 and minSup > minS_k:
        freqItems = []
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup)
        door_len = len(freqItems)

    if len(freqItems) > 9000:
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1)

    return freqItems


def middleFreqItems(dataSet, minS_k):
    door_len = 0
    minSup = 40
    while door_len < 1000 and minSup > minS_k:
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup)
        door_len = len(freqItems)

    if len(freqItems) < 30:
        freqItems = getfreqitem(dataSet=dataSet, minS=2)

    if len(freqItems) > 6000:
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1)

    return freqItems


def smallFreqItems(dataSet, minS_k):
    door_len = 0
    minSup = 10
    while door_len < 500 and minSup > minS_k:
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup)
        door_len = len(freqItems)

    if len(freqItems) > 2000:
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1)

    return freqItems


def packageFPGrowthRun(prov, subj, datetime, FO_PATH):
    ''' 普通过程的fpgrowth打包程序
                                @param prov         省份
                                @param subj         年级学科
                                @param FO_PATH      FPGrowth算法输出路径
                                @param datetime     日期区间
                            '''

    output_file = FO_PATH + '/' + fpgth_output_file.format(prov, subj, datetime)

    exist_file = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, subj, datetime)
    if os.path.exists(exist_file):
        logging.info(u"正在读取{0}省份下{1}学科的Prov_Sub_input文件的数据！".format(prov, subj))

        with open(exist_file,'r') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)

            if os.path.getsize(exist_file) > 2000000:
                freqItems = bigFreqItems(dataSet=data_json, minS_k=5)
            elif os.path.getsize(exist_file) < 200000:
                freqItems = smallFreqItems(dataSet=data_json, minS_k=1)
            else:
                freqItems = middleFreqItems(dataSet=data_json, minS_k=3)

            del data_json
            with open(output_file, 'wt') as csv_file:
                for fi in freqItems:
                    csv_file.writelines('--'.join(i for i in fi))
                    csv_file.write('\n')
                csv_file.close()

            ps_file.close()
        logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件！".format(prov, subj))


if __name__ == '__main__':
    datetimes = {'09-11'}

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    prov_set = getProvinceSet()
    # prov_set = {'浙江'}
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)} | {'52', '62'}

    pool = Pool(3)
    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Analy_fpgrowth_{}.log'.format(datetime), filemode='a')
        for prov in prov_set:
            FO_PATH = FPGROWTH_PATH + datetime + '/' + prov
            mkdir(FO_PATH)

            for subj in subj_set:
                pool.apply_async(packageFPGrowthRun, kwds={
                    "prov": prov,
                    "subj": subj,
                    "datetime": datetime,
                    "FO_PATH": FO_PATH
                })

    pool.close()
    pool.join()
