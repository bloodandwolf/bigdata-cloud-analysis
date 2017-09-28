# -*-- encoding=utf-8 --*-
#! /usr/bin/python3

import codecs
import pandas as pd
import xlsxwriter
import os
from util import plot_trend, main_function
import sys
from pandas import ExcelWriter


def __read_one_csv_file(inCsvFileName):
    opertorName = codecs.decode(bytes('运营商', 'gbk'), encoding='ISO-8859-1')
    lacName = codecs.decode(bytes('位置码', 'gbk'), encoding='ISO-8859-1')
    cidName = codecs.decode(bytes('基站编码', 'gbk'), encoding='ISO-8859-1')
    csName = codecs.decode(bytes('电话网络', 'gbk'), encoding='ISO-8859-1')
    imeiName = codecs.decode(bytes('imei（default）', 'gbk'), encoding='ISO-8859-1')
    modelName = codecs.decode(bytes('机型（default）', 'gbk'), encoding='ISO-8859-1')
    versionName = codecs.decode(bytes('系统版本（default）', 'gbk'), encoding='ISO-8859-1')
    callFailData = pd.read_csv(inCsvFileName,
                               dtype={imeiName: object,
                                      opertorName: object,
                                      lacName: object,
                                      cidName: object,
                                      csName: object,
                                      modelName: object,
                                      versionName: object},
                               encoding="ISO-8859-1", error_bad_lines=False, low_memory=False)
    columnsList = []
    for i in callFailData.columns:
        columnsList.append(codecs.decode(bytes(i, 'ISO-8859-1'), encoding="gbk"))
    callFailData.columns = columnsList

    return callFailData


def __read_csv_directory(inCsvFileName):
    '''
    遍历目录下所有的csv文件
    '''
    callFailData_list = []
    path_1 = os.path.abspath(inCsvFileName)
    print(path_1)
    for li in os.listdir(path_1):
        oldName = os.path.join(path_1, li)
        callFailData1 = __read_one_csv_file(oldName)
        callFailData_list.append(callFailData1)

    callFailData = callFailData_list[0]
    for i in range(1, len(callFailData_list)):
        callFailData = callFailData.append(callFailData_list[i], ignore_index=True)
    return callFailData


def __clean_data_all_data(callFailData):
    print('\n-----开始数据清理-----')
    shape_before = callFailData.shape[0]

    print('为所有的空值填充NULL...')
    callFailData = callFailData.fillna('null')

    callFailData['机型（default）'] = callFailData['机型（default）'].fillna('null')

    print('开始--移除 Normal cause...')
    fp = open(os.path.join(os.path.abspath('.'),'config', 'remove_items.txt'), 'r')
    allines = fp.readlines()
    for cause in allines:
        callFailData = callFailData[callFailData['失败原因'] != cause.strip()]

    print('----------------------------' + str(callFailData.shape[0]))
    
    shape_after_remove_cause = callFailData.shape[0]

    print('开始--移除 PLMN(99901/00000/00101/123456)和NULL PLMN...')
    callFailData = callFailData.loc[(callFailData["运营商"] != "99901") &
                                    (callFailData["运营商"] != "00000") &
                                    (callFailData["运营商"] != "00101") &
                                    (callFailData["运营商"] != "123456") &
                                    (callFailData["运营商"] != "null")]

    callFailData = callFailData.loc[(callFailData["imei（default）"] != "A100003A5028D8")]
    callFailData = callFailData.loc[(callFailData["imei（default）"] != "12345678912345")]

    print('开始--移除 0/1位置码...')
    callFailData = callFailData[callFailData['位置码'] != 0]
    callFailData = callFailData[callFailData['位置码'] != 1]
    callFailData = callFailData[callFailData['位置码'] != '0']
    callFailData = callFailData[callFailData['位置码'] != '1']

    print('开始--移除 0/1基站编码...')
    callFailData = callFailData[callFailData['基站编码'] != 0]
    callFailData = callFailData[callFailData['基站编码'] != 1]
    callFailData = callFailData[callFailData['基站编码'] != '0']
    callFailData = callFailData[callFailData['基站编码'] != '1']

    print('开始--移除 UNKNOWN的电话网络...')
    callFailData = callFailData[callFailData['电话网络'] != 'UNKNOWN']

    print('开始--移除 呼入呼出为NULL的数据...')
    callFailData = callFailData[callFailData['呼入或呼出'] != 'null']

    print('开始--对发生时间进行处理...')
    callFailData['发生时间t'] = pd.to_datetime(callFailData['事件发生时间（default）'], infer_datetime_format=True)
    callFailData['发生时间h'] = callFailData['发生时间t'].apply(__get_hour)

    print('开始--对经纬度进行处理...')
    callFailData['地址'] = callFailData['地址'].apply(__get_longitude_latitude)

    print('开始--对名字进行替换处理...')
    callFailData['imei'] = callFailData['imei（default）']
    callFailData['机型'] = callFailData['机型（default）']
    callFailData['系统版本'] = callFailData['系统版本（default）']

    print('处理sim卡...')
    callFailData['SIM卡'] = callFailData['SIM卡'].apply(__replace_sim)

    print('开始--组合自定义项处理...')
    callFailData['基站位置'] = callFailData['运营商'].str.cat(callFailData['位置码'], sep='-').str.cat(callFailData['基站编码'],
                                                                                             sep='-')
    callFailData['PLMN_CS'] = callFailData['运营商'].str.cat(callFailData['电话网络'], sep='-')
    callFailData['PLMN_PS'] = callFailData['运营商'].str.cat(callFailData['数据网络'], sep='-')
    callFailData['CS_PS_NW'] = callFailData['电话网络'].str.cat(callFailData['数据网络'], sep='-')
    callFailData['PLMN_CS_PS_NW'] = callFailData['运营商'].str.cat(callFailData['CS_PS_NW'], sep='-')

    callFailData['机型-版本'] = callFailData['机型（default）'].str.cat(callFailData['系统版本（default）'], sep='-')
    callFailData['测试区域'] = callFailData['机型'].str.cat(callFailData['基站位置'], sep='-')

    callFailData['通话类型'] = 'cs'

    data_every_file2 = callFailData[
        ['imei', '机型', '系统版本', 'SIM卡', '失败原因', '地址', '基站位置', '呼入或呼出', '运营商', '通话类型', '电话网络', '数据网络',
         '事件发生时间（default）']]

    data_every_file1 = callFailData.drop(['位置码', '基站编码', '数据网络', 'PLMN_PS', '预留字段1', '预留字段2', 
                                          '主键（default）', '事件发生时间（default）', '发生时间h','CS_PS_NW',
                                          'PLMN_CS_PS_NW', '测试区域','机型-版本',
                                          '发生时间t', '事件开始时间（default）', '事件结束时间（default）',
                                          '事件持续时间（default）', 'imei（default）', '机型（default）',
                                          '系统版本（default）'], axis=1)
    shape_after = callFailData.shape[0]
    print('-----数据清洗完成....' + str(shape_after) + '/' + str(shape_before) + '-----\n')
    return data_every_file1, data_every_file2, shape_after_remove_cause


def __replace_sim(sim):
    if (sim == 1.0):
        return '卡1'
    elif (sim == 2.0):
        return '卡2'
    else:
        return 'null'


def __get_hour(name):
    returnName = name.to_pydatetime().hour
    return returnName


def __get_address(name):
    string_list = []
    try:
        for ch in name:
            string_list.append(chr(ord(ch) - 5))
        return ''.join(string_list)
    except:
        return name


def __get_longitude_latitude(name):
    returnName = 'null'
    if (name != 'null'):
        try:
            for i in name:
                ch = codecs.decode(bytes(i, 'ISO-8859-1'), encoding="gbk")
                returnName += chr(ord(ch) - 5)
            return returnName
        except:
            return 'null'
    else:
        return 'null'


def bigDataIn_plot_trend(path_raw_data, path_result):
    sheet_name_list = ['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']
    trend_dics_list = {}
    trend_dics_list['SIM卡'] = ['卡1', '卡2']
    trend_dics_list['失败原因'] = ['CALL_END_CAUSE_RECOVERY_ON_TIMER_EXPIRED_V02', 'CALL_END_CAUSE_RADIO_LINK_LOST_V02', 'CALL_END_CAUSE_FADE_V02', 'ERROR_UNSPECIFIED_25', 'CALL_END_CAUSE_OPERATOR_DETERMINED_BARRING_V02']
    trend_dics_list['呼入或呼出'] = ['Incoming', 'Outgoing']
    trend_dics_list['运营商'] = ['46000', '46001', '46011']
    trend_dics_list['电话网络'] = ['GSM', 'CDMA - 1xRTT', 'UMTS', 'GPRS', 'EDGE',
                               'LTE', 'TD_SCDMA', 'HSPA', 'TD-SCDMA', 'HSDPA']
    trend_dics_list['机型'] = ['vivo X9i', 'vivo X9', 'vivo Xplay6',
                             'vivo Y67', 'vivo Y66', 'vivo X7']
    #trend_dics_list['PLMN_CS'] = []
    trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    plot_trend('大数据内销掉话', path_raw_data, path_result, trend_dics_list)


def big_data_in_call_fail_main(path_raw_data, path_result):
    main_function('大数据内销掉话', path_raw_data, path_result, __read_one_csv_file, __read_csv_directory,
                  __clean_data_all_data)


if __name__ == '__main__':
    in_path=os.path.abspath(os.path.abspath('.'))
    path_raw_data=os.path.join(in_path,'big_data_in_raw_data','big_data_in_raw_data_weeks')
    path_result=os.path.join(in_path,'big_data_in_report_data','big_data_in_report_data_weeks')

    big_data_in_call_fail_main(path_raw_data,path_result)

    in_path = os.path.abspath(os.path.abspath('.'))
    path_raw_data = os.path.join(in_path, 'big_data_in_report_data', 'big_data_in_report_data_weeks')
    path_result = os.path.join(in_path, 'big_data_in_report_data', 'big_data_in_report_data_weeks_trend.xls')

    bigDataIn_plot_trend(path_raw_data, path_result)

