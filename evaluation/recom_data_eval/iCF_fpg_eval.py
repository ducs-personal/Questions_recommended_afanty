# -*-coding:utf8-*-
import os
import sys
import time
import logging
from lib.table_data import tableToJson, getQuestionId
import csv
import json
import platform
from operator import itemgetter
from lib.table_evaluat import evalSubKpo
from running.Recom_data_run.recom_fpg_run import getRecomFPGth
from multiprocessing import Pool
from optparse import OptionParser
from lib.util import (
    mkdir,
    getRecomFPGth,
    getProvinceSet
)

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
RECOMMEND = _DIR + '/new_database/Recommend/'
ANALY = _DIR + '/new_database/Analy/'
EVALUATE = _DIR + '/new_database/Evaluate/'

USER_PATH = RECOMMEND + 'user/'
ANA_ICF_PATH = ANALY + 'ItemCF_output/'
EVA_ICF_PATH = EVALUATE + 'ItemCF_eval/'
ANA_FPG_PATH = ANALY + 'FPGrowth_output/'
EVA_FPG_PATH = EVALUATE + 'FPGrowth_eval/'
SUBKPT_PATH = EVALUATE + 'Subkpt_Knlkpt_eval/Raw_eval/'
SK_PATH = EVALUATE + 'Subkpt_Knlkpt_eval/Second_eval/'

ana_iCF_file = 'item_colf_{}_{}_{}.txt'
ana_fpg_file = 'fp_growth_{}_{}_{}.txt'
eva_iCF_file = 'itemCF_{}_{}_{}.txt'
eva_fpg_file = 'fpgrowth_{}_{}_{}.txt'
sub_kpt_skp_file = 'sub_kpt_skp_{}.txt'

output_file = 'output_{}_{}.csv'


def subjDic(kn_kpo):
    subj_dic = {}
    for sub_kpoint in kn_kpo:
        if str(sub_kpoint[0]) not in subj_dic.keys():
            subj_dic[str(sub_kpoint[0])] = [[sub_kpoint[1], sub_kpoint[2]]]
        else:
            subj_dic[str(sub_kpoint[0])].append([sub_kpoint[1], sub_kpoint[2]])

    return subj_dic


def getKnowList(vs):
    know_point_list = []
    knled_pt_list = [i[0].replace("．", '').replace(";", '；') for i in vs]
    for knled_pt in knled_pt_list:
        know_point_list.extend(knled_pt.split("；"))

    return know_point_list


def readICF(filename, input_list):
    output_set = set([])
    with open(filename, 'r', encoding='gbk') as recom_file:
        while True:
            recom = recom_file.readline()
            if recom:
                if list(eval(recom).keys())[0] in input_list:
                    for j, wj in sorted(
                            eval(recom)[list(eval(recom).keys())[0]].items(), key=itemgetter(1), reverse=True)[:3]:

                        if j in input_list:
                            continue
                        output_set.add(j)

            else:
                break

        recom_file.close()
    return output_set


def readFpg(filename, input_list, k):
    output_set = set([])
    with open(filename, 'r', encoding='gbk') as recom_file:
        while True:
            result = recom_file.readline()
            if result:
                output_set |= getRecomFPGth(result.replace('\n', '').split('--'), set(input_list), k=k)

            else:
                break

        recom_file.close()

    if len(output_set) < 1 and k < 1:
        readFpg(filename, input_list, k+1)
    else:
        return output_set


def getRecomSet(icf_file, fpg_file, vs, user_id):
    if os.path.exists(icf_file):
        if os.path.exists(fpg_file):
            sub_kponint_list = [i[1] for i in vs]

            recom_set_icf = readICF(icf_file, sub_kponint_list)
            recom_set_fpg = readFpg(fpg_file, sub_kponint_list, k=1)

            recom_set = recom_set_fpg | recom_set_icf
            if len(recom_set) == 0:
                recom_set = set(sub_kponint_list)
                print("用户{0}在Recommend下没有得到推荐结果".format(user_id))

            return recom_set

        else:
            print("Recommend下路径{0}不存在！".format(fpg_file))
            return set([])

    else:
        print("Recommend下路径{0}不存在！".format(icf_file))
        return set([])


def getEvalSet(icf_file, fpg_file, vs, user_id):
    if os.path.exists(icf_file):
        if os.path.exists(fpg_file):
            know_point_list = getKnowList(vs)

            eval_set_icf = readICF(icf_file, know_point_list)
            eval_set_fpg = readFpg(fpg_file, know_point_list, k=1)

            eval_set = eval_set_fpg | eval_set_icf
            if len(eval_set) == 0:
                eval_set = set(know_point_list)
                print("用户{0},在Evalution下没有得到推荐结果".format(user_id))

            return eval_set
        else:
            print("Evalution下路径{0}不存在!".format(fpg_file))
            return set([])

    else:
        print("Evalution下路径{0}不存在!".format(icf_file))
        return set([])


def dealEvalSet(ks, eval_set):
    new_set = set([])

    with open(SK_PATH + sub_kpt_skp_file.format(ks), 'r', encoding='utf-8') as second_file:
        sk_dict = eval(second_file.readlines()[0])
        second_file.close()

    # new_sub = set([])
    # new_add = set([])
    #
    # for es1 in eval_set:
    #     for es2 in eval_set:
    #         if es1 in sk_dict.keys() and es2 in sk_dict.keys() and es1 != es2:
    #             if len(sk_dict[es2] & sk_dict[es1]) > 0:
    #                 new_set |= sk_dict[es2] & sk_dict[es1]
    #                 new_add.add(es1)
    #                 new_add.add(es2)
    #
    # new_sub = eval_set - new_add
    # for es in new_sub:
    #     if es in sk_dict.keys():
    #         new_set |= sk_dict[es]

    for e in eval_set:
        if e in sk_dict.keys():
            new_set |= sk_dict[e]

    return new_set


def EvaluatSubKpoint(recom_set, eval_set, ks):
    eval_set = dealEvalSet(ks, eval_set)

    AC_len = len(recom_set) + 1
    AB_len = len(eval_set) + 1
    A_count = len(recom_set & eval_set)

    precision = A_count / AC_len
    recall = A_count / AB_len

    return precision, recall


def dealRecomEval(ks, recom_set):
    new_set = set([])

    with open(SUBKPT_PATH + sub_kpt_skp_file.format(ks), 'r', encoding='utf-8') as raw_file:
        sk_dict = eval(raw_file.readlines()[0])
        raw_file.close()

    for rs in recom_set:
        if rs in sk_dict.keys():
            new_set |= sk_dict[rs]

    return new_set


def EvaluatKnowPoint(recom_set, eval_set, ks):
    recom_set = dealRecomEval(ks, recom_set)
    AC_len = len(eval_set) + 1
    AB_len = len(recom_set) + 1
    A_count = len(recom_set & eval_set)

    precision = A_count / AC_len
    recall = A_count / AB_len

    return precision, recall


def packageRcomItemCF(req_user, datetime):
    ''' 对推荐试题结果程序进行封装打包
                                @param req_user     原始数据文件(user_id, province， question)
                                @param datetime     时间区间
                            '''
    prov_set = getProvinceSet()

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

            kn_kpo = evalSubKpo(
                mode=0,
                data=question_id,
                table='question_simhash_20171111'
            )

            if len(kn_kpo) > 0:
                subj_dic = subjDic(kn_kpo)

                for ks, vs in subj_dic.items():
                    ana_icf = ANA_ICF_PATH+ datetime + '/' + prov + '/' + ana_iCF_file.format(prov,ks, datetime)
                    ana_fpg = ANA_FPG_PATH + datetime + '/' + prov + '/' + ana_fpg_file.format(prov, ks, datetime)
                    eval_icf = EVA_ICF_PATH + datetime + '/' + prov + '/' + eva_iCF_file.format(prov,ks, datetime)
                    eval_fpg = EVA_FPG_PATH + datetime + '/' + prov + '/' + eva_fpg_file.format(prov,ks, datetime)

                    recom_set = getRecomSet(ana_icf, ana_fpg, vs, user_id)
                    eval_set = getEvalSet(eval_icf, eval_fpg, vs, user_id)

                    if len(recom_set) > 0 and len(eval_set) > 0:
                        Sprecision, Srecall = EvaluatSubKpoint(recom_set, eval_set, ks)
                        Kprecision, Krecall = EvaluatKnowPoint(recom_set, eval_set, ks)
                        print(Sprecision, Srecall)
                        print(Kprecision, Krecall)
                        print("# # # " * 20)

                    else:
                        logging.error("至少有一个路径下没有文件，可能是datetimes、prov、subj的问题!")

                    logging.info("已经评测完用户{0}年级科目{1}！".format(user_id, ks))

            else:
                logging.error(
                    "用户{}传入的question_id在表question_simhash_20171111里查询不到！".format(user_id))

        user_file.close()


if __name__ == '__main__':
    if 'Windows' in platform.system():
        req_user = 'requir_user.csv'
        datetimes = {'09-11'}
        pool = Pool(2)

    elif 'Linux' in platform.system():
        optparser = OptionParser()
        optparser.add_option('-f', '--inputfile',
                             dest='input',
                             help='必须是csv格式的文件, 如requir_user.csv',
                             default='requir_user.csv')
        optparser.add_option('-t', '--datetimes',
                             dest='datetimes',
                             help='包含时间区间的集合,若是多个时用 , （英文逗号）分割开',
                             default='09-11')

        (options, args) = optparser.parse_args()

        req_user = options.input
        if not os.path.exists(USER_PATH + req_user):
            sys.exit("该路径下{0}下没有{1}文件，请将文件放到上面路径下".format(USER_PATH, req_user))

        datetimes = set(options.datetimes.replace(' ','').split(','))
        pool = Pool(4)

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Rec_iCF_fpg_{}.log'.format(datetime), filemode='a')

        pool.apply_async(packageRcomItemCF, kwds={
            "datetime": datetime,
            "req_user": req_user
        })
        # packageRcomItemCF(
        #     datetime=datetime,
        #     req_user=req_user
        # )

    pool.close()
    pool.join()



