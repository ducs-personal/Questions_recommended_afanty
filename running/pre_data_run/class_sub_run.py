# -*-coding:utf8-*-
import os
import sys
import csv
import logging
import json
import platform
from optparse import OptionParser
from multiprocessing import Pool
from lib.util import getProvinceSet,mkdir

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
SUB_KPOINT_PATH = DATABASE + 'Sub_kpoint_input/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
prov_subj_file = 'prov_subject_{}_{}_{}.txt'

def packClassSub(prov, datetime, P_S_PATH):
    skp_file = 'question_sub_kpoint_{}_{}.txt'
    exit_file = SUB_KPOINT_PATH + datetime + '/' + skp_file.format(prov, datetime)

    if os.path.exists(exit_file):
        with open(exit_file, 'r') as sub_kpoint_file:
            while True:
                line = sub_kpoint_file.readline()
                if line:
                    data_json = eval(line)

                    sub_dic = {}
                    for i in range(1, len(data_json)):
                        if data_json[i][1] not in sub_dic.keys():
                            sub_dic[data_json[i][1]] = [data_json[0], data_json[i][0]]
                        else:
                            sub_dic[data_json[i][1]].append(data_json[i][0])

                    for subj in sub_dic.keys():
                        if len(list(sub_dic.values())[0]) > 3:
                            with open(P_S_PATH + '/' + prov_subj_file.format(prov, subj, datetime), 'a') as new_file:
                                new_file.writelines(json.dumps(list(sub_dic.values())[0]) + '\n')

                else:
                    break

            logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到Prov_Sub_input文件！".format(prov, subj))
            new_file.close()
            sub_kpoint_file.close()

    else:
        logging.error("该路径下{0}没有文件!".format(exit_file))


if __name__ == '__main__':
    if 'Windows' in platform.system():
        datetimes = {'09-11'}
        pool = Pool(2)

    elif 'Linux' in platform.system():
        optparser = OptionParser()
        optparser.add_option('-t', '--datetimes',
                             dest='datetimes',
                             help='包含时间区间的集合,若是多个时用 , （英文逗号）分割开',
                             default='09-11')
        (options, args) = optparser.parse_args()

        datetimes = set(options.datetimes.replace(' ','').split(','))
        pool = Pool(4)

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    prov_set = getProvinceSet()
    # prov_set = {'青海'}
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Input_class_sub_{}.log'.format(datetime), filemode='a')
        for prov in prov_set:
            P_S_PATH = PROV_SUB_PATH + datetime + '/' + prov
            mkdir(P_S_PATH)
            logging.info("Classify the {0} of subject at the time between {1}".format(prov, datetime))

            for subj in subj_set:
                filename = P_S_PATH + '/' + prov_subj_file.format(prov, subj, datetime)
                if os.path.exists(filename):
                    os.remove(filename)

            pool.apply_async(packClassSub, kwds={
                "prov": prov,
                "datetime":datetime,
                "P_S_PATH": P_S_PATH
            })

    pool.close()
    pool.join()
