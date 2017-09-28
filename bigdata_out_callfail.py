# encoding=utf-8

import os
import json
import pandas as pd
import xlsxwriter
from util import main_function, plot_trend


def __read_one_csv_file(inCsvFileName):
    print('read...' + inCsvFileName)
    fp = open(inCsvFileName, 'r')
    allLines = fp.readlines()
    fp.close()

    if (len(allLines) == 0):
        print('file is empty...')
        return None

    imeiList = []
    modelList = []
    systemVersionList = []
    timeStampList1 = []
    callFailItemsJsonList = []
    for currentLine in allLines:
        items = currentLine.split('\t')

        imeiList.append(items[0])
        modelList.append(items[1])
        systemVersionList.append(items[2])
        timeStampList1.append(items[6])
        callFailItemsJsonList.append(items[14])

    callFailDataJson = pd.DataFrame.from_records(map(json.loads, callFailItemsJsonList))

    callFailData_part1 = pd.DataFrame({'imei': imeiList,
                                       'model': modelList,
                                       'systemVersion': systemVersionList,
                                       'timeStamp_happen': timeStampList1})

    callFailData = callFailData_part1.join(callFailDataJson)

    return callFailData


def __read_csv_directory(inCsvFileName):
    callFailDataList = []
    file_path_PD1612 = os.path.join(inCsvFileName, 'PD1612_804_8041_通话失败收集.txt')
    file_path_PD1613 = os.path.join(inCsvFileName, 'PD1613_804_8041_通话失败收集.txt')
    file_path_PD1624 = os.path.join(inCsvFileName, 'PD1624_804_8041_通话失败收集.txt')

    callFailData1 = __read_one_csv_file(file_path_PD1612)
    callFailData2 = __read_one_csv_file(file_path_PD1613)
    callFailData3 = __read_one_csv_file(file_path_PD1624)

    if (callFailData1 is None):
        pass
    else:
        callFailDataList.append(callFailData1)
    if (callFailData2 is None):
        pass
    else:
        callFailDataList.append(callFailData2)
    if (callFailData3 is None):
        pass
    else:
        callFailDataList.append(callFailData3)

    if (callFailData1 is None and callFailData2 is None and callFailData3 is None):
        return None
    else:
        print('开始合不同文件的数据')
        callFailData = callFailDataList[0]
        for i in range(1, len(callFailDataList)):
            callFailData = callFailData.append(callFailDataList[i], ignore_index=True)

        print('合并文件之后的大小为=' + str(callFailData.shape[0]))

        return callFailData


def __clean_data_all_data(callFailData):
    print('开始clean data...')
    callFailData = callFailData.fillna('null')

    # 失败原因
    print('开始清除正常cause...')
    fp = open(os.path.join('.', 'config', 'remove_items.txt'), 'r')
    allines = fp.readlines()

    for cause in allines:
        callFailData = callFailData[callFailData['fa'] != cause.strip()]

    print('--------------------------------------------' + str(callFailData.shape[0]))
    shape_remove_normal_cause = callFailData.shape[0]

    # 运营商
    print('开始清除特殊PLMN...')
    callFailData = callFailData.loc[(callFailData["pl"] != "99901") &
                                    (callFailData["pl"] != "00000") &
                                    (callFailData["pl"] != "00101") &
                                    (callFailData["pl"] != "123456") &
                                    (callFailData["pl"] != "null")]

    print('开始清除特殊LAC CID...')
    callFailData = callFailData.loc[(callFailData["la"] != 0) &
                                    (callFailData["la"] != 1)]

    callFailData = callFailData.loc[(callFailData["ci"] != 0) &
                                    (callFailData["ci"] != 1)]

    # callFailData = callFailData.loc[(callFailData["cs"] != 'UNKNOWN')]
    # callFailData = callFailData.loc[(callFailData["cs"] != 'CDMA - 1xRTT')]

    print('开始清除特殊IMEI...')
    callFailData = callFailData.loc[(callFailData["imei"] != '123456789012345')]

    callFailData.loc[callFailData['model'] == 'vivo 1610', 'model'] = 'PD1613BF_EX'
    callFailData.loc[callFailData['model'] == 'vivo 1601', 'model'] = 'PD1612F_EX'
    callFailData.loc[callFailData['model'] == 'vivo 1603', 'model'] = 'PD1613F_EX'
    callFailData.loc[callFailData['model'] == 'vivo 1713', 'model'] = 'PD1612DF_EX'

    print('开始转换PLMN MCC...')
    callFailData['mcc'] = callFailData['pl'].apply(__get_mcc)
    filename1 = os.path.join('.', 'config', 'plmn.txt')
    fp1 = open(filename1, 'r')
    for line in fp1.readlines():
        split_list = line.split('\t')
        callFailData.loc[callFailData['mcc'] == split_list[1], 'country'] = split_list[0]
        callFailData.loc[callFailData['pl'] == split_list[1] + split_list[2], 'PLMN2'] = split_list[1] + split_list[
            2] + '=' + split_list[0] + split_list[3] + '(' + split_list[4].strip() + ')'

    print('开始合并统计项....')
    callFailData['PLMN_LAC_CID'] = callFailData['pl'].str.cat(callFailData['la'], sep='-').str.cat(callFailData['ci'],
                                                                                                   sep='-')
    callFailData['PLMN_CS'] = callFailData['pl'].str.cat(callFailData['cs'], sep='-')
    # 'fa','la','ci','cs','io','pl'
    callFailData['failCause'] = callFailData['fa']
    callFailData['LAC'] = callFailData['la']
    callFailData['CellID'] = callFailData['ci']
    callFailData['CS_NW'] = callFailData['cs']
    callFailData['Direction'] = callFailData['io']
    callFailData['PLMN'] = callFailData['pl']
    callFailData['SIM'] = callFailData['si']

    callFailData['PLMN_CS_PS_NW'] = callFailData['PLMN_CS'].str.cat(callFailData['ps'], sep='-')

    callFailData['机型'] = callFailData['model']

    print('开始过滤时间戳...')
    # callFailData['timeStamp']=pd.to_datetime(callFailData['timeStamp_happen'],infer_datetime_format=True)
    # callFailData['timeStamp']=callFailData['timeStamp'].apply(__getHour)

    rawlength = callFailData.shape[0]

    data_every_file = callFailData.drop(['model', 'ad', 'ci', 'cs', 'fa', 'io', 'la', 'pl',
                                         'ps', 're1', 're2', 'si', 'timeStamp_happen'], axis=1)
    return data_every_file, data_every_file, shape_remove_normal_cause


def __get_mcc(name):
    return (name[:3])

def __getHour(name):
    returnName = name.to_pydatetime().hour
    return returnName

def __process_different_model_num(callFailData, work_dir, inCsvFileName_head):
    modelList = callFailData['model'].unique().tolist()

    # print('为所有的空值填充NULL...')
    callFailData = callFailData.fillna('null')

    # print('开始--移除 Normal cause...')
    fp = open(os.path.join('.', '配置信息', 'remove_items.txt'), 'r')
    allines = fp.readlines()
    for cause in allines:
        callFailData = callFailData[callFailData['failCause'] != cause.strip()]

    xls_fileName = os.path.join(work_dir, '趋势数据', inCsvFileName_head + '.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)
    worksheet = workbook.add_worksheet('counter')
    #     worksheet.write(0,0,'model')
    #     worksheet.write(0,1,'counter')

    for model_i in range(len(modelList)):
        private_callFailData = callFailData[callFailData['model'] == modelList[model_i]]
        count = private_callFailData.shape[0]

        worksheet.write(model_i + 1, 0, modelList[model_i])
        worksheet.write(model_i + 1, 1, count)

    chart = workbook.add_chart({'type': 'line'})
    categories = '=counter!$A$2:$A$' + str(len(modelList) + 1)
    name = '==counter!$A$0'
    values = '==counter!$B$2:$B' + str(len(modelList) + 1)
    chart.add_series({
        'name': name,
        'categories': categories,
        'values': values,
        'marker': {'type': 'automatic'},
        'data_labels': {'value': True},
    })
    worksheet.insert_chart('D2', chart)

    for model_i in range(len(modelList)):

        worksheet = workbook.add_worksheet(modelList[model_i])
        worksheet.set_column('A:A', 80)

        private_callFailData = callFailData[callFailData['model'] == modelList[model_i]]
        count = private_callFailData.shape[0]

        worksheet.write(model_i + 1, 0, modelList[model_i])
        worksheet.write(model_i + 1, 1, count)

        failCauseData = private_callFailData['fa']
        allItemsSorted = failCauseData.value_counts()
        causeList = allItemsSorted.index.tolist()
        counterList = allItemsSorted.values.tolist()

        for i in range(0, len(causeList)):
            worksheet.write(i, 0, (causeList[i]))
            worksheet.write(i, 1, str(counterList[i]))

    workbook.close()


def big_data_out_call_fail_main(path_raw_data, path_result):
    main_function('大数据外销掉话', path_raw_data, path_result, __read_one_csv_file, __read_csv_directory,
                  __clean_data_all_data)


def big_data_out_plot_trend(path_raw_data, path_result):
    sheet_name_list = ['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']
    trend_dics_list = {}
    trend_dics_list['SIM'] = ['1', '2']

    trend_dics_list['failCause'] = ['CALL_END_CAUSE_RECOVERY_ON_TIMER_EXPIRED_V02', 'ERROR_UNSPECIFIED',
                                    'CALL_END_CAUSE_RLF_DURING_CC_DISCONNECT_V02', 'CALL_END_CAUSE_FADE_V02',
                                    'CALL_END_CAUSE_RADIO_LINK_LOST_V02']

    trend_dics_list['Direction'] = ['Incoming', 'Outgoing']

    trend_dics_list['PLMN2'] = ['51010=印尼Telkomsel(PT Telekomunikasi Selular)',
                                '40411=印度Vodafone India(Delhi & NCR)',
                                '52003=泰国AIS(Advanced Wireless Network Company Ltd.)',
                                '40410=印度AirTel(Delhi & NCR)',
                                '51503=菲律宾SMART(PLDT via?Smart Communications)']

    trend_dics_list['CS_NW'] = ['GSM', 'UMTS', 'HSPA', 'LTE', 'GPRS', 'EDGE', 'HSDPA', 'HSUPA']

    trend_dics_list['机型'] = ['vivo 1603', 'vivo 1610', 'vivo 1601']

    trend_dics_list['systemVersion'] = ['PD1613BF_EX_PD1613BF_EXMA_1.11.3', 'PD1613F_EX_PD1613F_EXMA_1.15.4', 'PD1612F_EX_PD1612F_EXMA_2.9.1', 'PD1613F_EX_PD1613F_EXMA_1.15.10', 'PD1612F_EX_PD1612F_EXMA_2.7.17']

    trend_dics_list['country'] = ['印度', '印尼', '泰国', '菲律宾', '缅甸', '越南', '智利', '尼泊尔', '马来西亚', '孟加拉国']

    trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    plot_trend('大数据外销掉话', path_raw_data, path_result, trend_dics_list)


if __name__ == '__main__':
    abspath = os.path.abspath(
        'D:/tools/pycharm_projects/bigdata_analysis/big_data_out_raw_data/volte_2017-06-25_2017-07-01/test')
    big_data_out_call_fail_main(abspath, abspath)
