# -*-coding:utf8-*-
import os
import sys
import numpy as np
import time
import logging
from lib.table_data import tableToJson, getQuestionId
import pandas as pd
import csv
import json
from operator import itemgetter
from running.Recom_data_run.recom_fpg_run import getRecomFPGth
from multiprocessing import Pool
from lib.util import (
    mkdir,
    getRecomFPGth,
    getProvinceSet
)

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
RECOMMEND = _DIR + '/new_database/Recommend/'
ANALY = _DIR + '/new_database/Analy/'

USER_PATH = RECOMMEND + 'user/'
ITEMCF_PATH = ANALY + 'ItemCF_output/'
FPGTH_PATH = ANALY + 'FPGrowth_output/'

itemCF_file = 'item_colf_{}_{}_{}.txt'
fpgth_output_file = 'fp_growth_{}_{}_{}.txt'
output_file = 'output_{}_{}.csv'


def subjDic(question_list, subj_dic):
    for sub_kpoint in question_list:
        if str(sub_kpoint[1]) not in subj_dic.keys():
            subj_dic[str(sub_kpoint[1])] = [sub_kpoint[0]]
        else:
            subj_dic[str(sub_kpoint[1])].append(sub_kpoint[0])

    return subj_dic


def readAnaICF(ana_icf, vs, recom_set_itemCF):
    with open(ana_icf, 'r', encoding='utf-8') as recom_file:
        while True:
            recom = recom_file.readline()
            if recom:
                if list(eval(recom).keys())[0] in vs:
                    for j, wj in sorted(
                            eval(recom)[list(eval(
                                recom).keys())[0]].items(), key=itemgetter(1), reverse=True)[:2]:

                        if j in vs:
                            continue
                        recom_set_itemCF.add(j)

            else:
                break
        recom_file.close()
    return recom_set_itemCF


def readAnaFpg(ana_fpg, vs, recom_set_fpg):
    with open(ana_fpg, 'r', encoding='utf-8') as recom_file:
        while True:
            result = recom_file.readline()
            if result:
                recom_set_fpg.add(getRecomFPGth(result.replace('\n', '').split('--'), set(vs)))

            else:
                break
    return recom_set_fpg


# 结合item_cf和FPGrowth两种推荐算法结果进行推荐
def getRecomSet(recom_set_itemCF, recom_set_fpg, recom_set, vs):
    if len(recom_set_itemCF) == 0 or len(recom_set_fpg) == 0:
        recom_set = recom_set_fpg | recom_set_itemCF

        if len(recom_set) == 0:
            recom_set = set(vs)

    else:
        recom_set = recom_set_fpg & recom_set_itemCF

        if len(recom_set) <= len(set(vs)):
            recom_set = recom_set_itemCF | recom_set_fpg

    return recom_set


def packageRcomItemCF(req_user, diff, datetime):
    ''' 对推荐试题结果程序进行封装打包
                                @param req_user     原始数据文件(user_id, province， question)
                                @param diff         难度系数，int
                                @param datetime     时间区间
                            '''
    prov_set = getProvinceSet()
    diff = diff or 1

    filename = USER_PATH + output_file.format(datetime, time.strftime("%Y%m%d"))
    if os.path.exists(filename):
        os.remove(filename)


    with open(USER_PATH + req_user, 'r', encoding='gbk') as user_file:
        readers = csv.DictReader(user_file)
        for reader in readers:
            prov = reader['province'][:2]
            if prov not in prov_set:
                prov = '全国'

            user_id = reader['user_id']
            question_id = eval(reader['question'])

            question_list = tableToJson(
                table='question_simhash_20171111',
                question_id=question_id
            )

            if len(question_list) > 0:
                subj_dic = {}
                recom_set = set([])

                subj_dic = subjDic(question_list, subj_dic)

                for ks, vs in subj_dic.items():
                    itemcf_file = ITEMCF_PATH + datetime + '/' + prov + '/' + itemCF_file.format(prov,ks, datetime)
                    fpg_file = FPGTH_PATH + datetime + '/' + prov + '/' + fpgth_output_file.format(prov,ks,datetime)

                    if os.path.exists(itemcf_file) or os.path.exists(fpg_file):
                        recom_set_itemCF = set([])
                        recom_set_fpg = set([])

                        recom_set_itemCF = readAnaICF(itemcf_file, vs, recom_set_itemCF)
                        recom_set_fpg = readAnaFpg(fpg_file, vs, recom_set_fpg)

                        recom_set = getRecomSet(recom_set_itemCF,recom_set_fpg, recom_set, vs)

                        # print(user_id, recom_set)
                        recom_list = getQuestionId(
                            table='question_simhash_20171111',
                            question_id=question_id,
                            recom_set=recom_set,
                            diff=diff
                        )
                        # print(user_id, recom_list)

                        with open(filename, 'a') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow([user_id, ks, recom_list])

                        logging.info("已经解析完用户{}！".format(user_id))

                    else:
                        logging.error(
                            "用户{0}在年级科目{1}下没有查询到ItemCF和FPGrowth输出表".format(user_id, ks))

            else:
                logging.error(
                    "用户{}传入的question_id在表question_simhash_20171111里查询不到！".format(user_id))

        user_file.close()


if __name__ == '__main__':
    req_user = 'requir_user.csv'
    datetimes = {'09-11'}
    diff = 1

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    pool = Pool(2)

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Rec_iCF_fpg_{}.log'.format(datetime), filemode='a')

        pool.apply_async(packageRcomItemCF, kwds={
            "diff":diff,
            "datetime":datetime,
            "req_user":req_user
        })
        # packageRcomItemCF(req_user,diff,datetime)

    pool.close()
    pool.join()