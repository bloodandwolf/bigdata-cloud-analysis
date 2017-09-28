# encoding=utf-8

import pandas as pd
import xlsxwriter
import os
from pandas import ExcelWriter
from util import plot_trend, main_function


def __readCsvOneFile(inCsvFileName):
    try:
        callFailData = pd.read_csv(inCsvFileName,
                                   dtype={'log信息': object,'掉网时长': object,'运营商': object, 'imei': object, '起呼位置码': object, '起呼基站编号': object})
        return callFailData
    except:
        print('read error ... ')
        return None


def __readCsvFile(inCsvFileName):
    if (os.path.isfile(inCsvFileName)):
        print('input is a file')
        callFailData = __readCsvOneFile(inCsvFileName)
    elif (os.path.isdir(inCsvFileName)):
        print('input is a isdir')
        callFailDataList = []
        absPath = os.path.abspath(inCsvFileName)
        for li in os.listdir(absPath):
            oldName = os.path.join(absPath, li)
            print(oldName)

            callFailData1 = __readCsvOneFile(oldName)
            if callFailData1 is not None:
                callFailDataList.append(callFailData1)

        callFailData = callFailDataList[0]
        for i in range(1, len(callFailDataList)):
            callFailData = callFailData.append(callFailDataList[i], ignore_index=True)

    print(callFailData.head(1))
    return callFailData


def __clean_data_cause(callFailData):
    callFailData = callFailData.fillna('null')

    callFailData = callFailData.drop(['内部机型', '系统版本', 'emmcid', 'imei',
                                      '发生时间', '上报时间', '异常进程名',
                                      '进程版本名', '进程版本号', '异常进程包名', '软件系统类型',
                                      '国家', '省/直辖市', '市', '县/区', '详细地址', '异常类型',
                                      '出现异常的卡', '失败原因', '呼入呼出', '起呼位置码', '起呼基站编号',
                                      '起呼电话网络', '开始数据网络', '运营商', '结束位置码',
                                      '结束基站编号', '结束电话网络', '结束数据网络', 'isim支持情况',
                                      'MBN版本信息', 'VOLTE配置信息', '是否volte', '呼叫对方号码',
                                      '保留字段一', '保留字段二', '异常次数', '日志路径', 'log信息'], axis=1)
    return callFailData


def __clean_data_all_data(callFailData):
    callFailData = callFailData.fillna('null')

    print('-----------------------------' + str(callFailData.shape[0]))
    shape = callFailData.shape[0]

    callFailData['PLMN_LAC1_CID1'] = callFailData['运营商'].str.cat(callFailData['起呼位置码'], sep='-').str.cat(
        callFailData['起呼基站编号'], sep='-')
    callFailData['PLMN_CS1'] = callFailData['运营商'].str.cat(callFailData['起呼电话网络'], sep='-')
    callFailData['PLMN_PS1'] = callFailData['运营商'].str.cat(callFailData['开始数据网络'], sep='-')
    callFailData['CS_NW'] = callFailData['起呼电话网络'].str.cat(callFailData['结束电话网络'], sep='-')
    callFailData['PS_NW'] = callFailData['开始数据网络'].str.cat(callFailData['结束数据网络'], sep='-')
    callFailData['CS_PS_NW'] = callFailData['CS_NW'].str.cat(callFailData['PS_NW'], sep='-')

    # 运营商
    callFailData = callFailData[callFailData['运营商'] != '99901']
    callFailData = callFailData[callFailData['运营商'] != '00000']
    callFailData = callFailData[callFailData['运营商'] != '00101']
    callFailData = callFailData[callFailData['运营商'] != '123456']
    callFailData = callFailData[callFailData['运营商'] != 'null']

    #
    callFailData = callFailData[callFailData['起呼位置码'] != 0]
    callFailData = callFailData[callFailData['起呼位置码'] != 1]
    callFailData = callFailData[callFailData['起呼位置码'] != '0']
    callFailData = callFailData[callFailData['起呼位置码'] != '1']

    #
    callFailData = callFailData[callFailData['起呼基站编号'] != 0]
    callFailData = callFailData[callFailData['起呼基站编号'] != 1]
    callFailData = callFailData[callFailData['起呼基站编号'] != '0']
    callFailData = callFailData[callFailData['起呼基站编号'] != '1']

    callFailData = callFailData[callFailData['起呼电话网络'] != 'Unknown']
    callFailData = callFailData[callFailData['开始数据网络'] != 'Unknown']

    rowLength_before = callFailData.shape[0]

    callFailData['结束网络'] = callFailData['结束电话网络'].str.cat(callFailData['结束数据网络'], sep='-')
    callFailData['掉网类型'] = callFailData['结束网络'].apply(__get_oos_type)
    # callFailData['掉网类型']=map(lambda x,y:__do_merchant(x,y),callFailData['结束电话网络'],callFailData['结束数据网络'])

    callFailData['开始电话网络'] = callFailData['起呼电话网络']
    callFailData['开始位置码'] = callFailData['起呼位置码']
    callFailData['开始基站编号'] = callFailData['起呼基站编号']

    callFailData['发生时间t'] = pd.to_datetime(callFailData['发生时间'], infer_datetime_format=True)
    callFailData['发生时间h'] = callFailData['发生时间t'].apply(__getHour)

    callFailData.loc[callFailData['保留字段一'] == 0, '保留字段一'] = '15'
    callFailData.loc[callFailData['保留字段一'] == 1, '保留字段一'] = '3'
    callFailData['掉网时长'] = callFailData['保留字段一']

    callFailData.loc[callFailData['出现异常的卡'] == 0, '出现异常的卡'] = '卡1'
    callFailData.loc[callFailData['出现异常的卡'] == 1, '出现异常的卡'] = '卡2'

    callFailData['机型'] = callFailData['外部机型']
    callFailData.loc[callFailData['地区码'] == 'in', '地区码'] = '印度'
    callFailData.loc[callFailData['地区码'] == 'ph', '地区码'] = '菲律宾'
    callFailData.loc[callFailData['地区码'] == 'th', '地区码'] = '泰国'
    callFailData.loc[callFailData['地区码'] == 'vn', '地区码'] = '越南'
    callFailData.loc[callFailData['地区码'] == 'id', '地区码'] = '印度尼西亚'
    callFailData.loc[callFailData['地区码'] == 'my', '地区码'] = '马来西亚'
    callFailData.loc[callFailData['地区码'] == 'pk', '地区码'] = '巴基斯坦'
    callFailData.loc[callFailData['地区码'] == 'mm', '地区码'] = '缅甸'
    callFailData.loc[callFailData['地区码'] == 'kh', '地区码'] = '柬埔寨'
    callFailData.loc[callFailData['地区码'] == 'tw', '地区码'] = '台湾'

    callFailData['失败原因1'] = callFailData['log信息']
    callFailData['失败类型'] = callFailData['失败原因']

    callFailData['信号强度']=callFailData['呼叫对方号码'].apply(__getRSRP)
    
    callFailData['失败原因2'] = callFailData['失败类型'].str.cat(callFailData['失败原因1'], sep='/')
    callFailData['失败时长'] = callFailData['失败原因2'].str.cat(callFailData['掉网时长'], sep='/')
    callFailData['失败信号'] = callFailData['失败原因2'].str.cat(callFailData['起呼电话网络'], sep='/').str.cat(callFailData['开始数据网络'], sep='/').str.cat(callFailData['信号强度'], sep='/')
    callFailData['失败信号2'] = callFailData['失败类型'].str.cat(callFailData['起呼电话网络'], sep='/').str.cat(callFailData['开始数据网络'], sep='/').str.cat(callFailData['信号强度'], sep='/')
    
    callFailData['地区机型'] = callFailData['地区码'].str.cat(callFailData['机型'], sep='/')
    
    rowLength_after = callFailData.shape[0]
    print('数据量大小为：' + str(rowLength_before) + '/' + str(rowLength_after))

    callFailData=callFailData.drop(['外部机型','内部机型','emmcid','地区码','上报时间','异常进程名','进程版本名',
                                    '进程版本号','异常进程包名','软件系统类型','异常类型','isim支持情况',
                                    'MBN版本信息','VOLTE配置信息','保留字段一','保留字段二','呼叫对方号码',
                                    '异常次数','日志路径','log信息','省/直辖市','县/区','发生时间','市',
                                    '县区','发生时间t','起呼位置码','结束位置码','起呼基站编号','结束基站编号',
                                    '起呼电话网络','开始数据网络','结束电话网络','结束数据网络','发生时间h','市1','县区1',
                                    'cell_add1','开始位置码','开始基站编号','国家','呼入呼出','起呼电话网络','开始数据网络',
                                    '是否volte','PLMN_CS1','PLMN_PS1','开始电话网络','失败原因'
                                    ],axis=1)

    return callFailData, callFailData, shape

def __getRSRP(name):
    name=str(name)
    returnName = name.strip()
    rsrp_list = []
    returnValue = 0
    if(name=='-1' or name=='null'):
        returnValue = str(-1)
    else:
        rsrp_list = returnName.split(',')
        min = 0
        for i in rsrp_list[:-2]:
            temp = eval(i)
            if(min > temp):
                min = temp
        returnValue = int(min / 5) * 5
    return str(returnValue)

def __get_mcc(name):
    return (name[:3])


def __do_merchant(x, y):
    if (x == 'Unknown' and y != 'Unknown'):
        return ('CS掉网')
    elif (x != 'Unknown' and y == 'Unknown'):
        return ('PS掉网')
    elif (x == 'Unknown' and y == 'Unknown'):
        return ('CSPS掉网')
    else:
        return ('null')


def __get_oos_type(name):
    if (name == 'Unknown-Unknown'):
        return ('CS+PS')
    elif (name.startswith('Unknown')):
        return ('CS')
    elif (name.endswith('Unknown')):
        return ('PS')
    else:
        return ('null')


def __getHour(name):
    returnName = name.to_pydatetime().hour
    return returnName


def __read_one_csv_file(inCsvFileName):
    try:
        callFailData = pd.read_csv(inCsvFileName,
                                   dtype={'运营商': object,
                                          'imei': object, '起呼位置码': object,
                                          '起呼基站编号': object, '结束位置码': object,
                                          '结束基站编号': object})
        # print(callFailData.columns)
        # print(callFailData.shape)
        return callFailData
    except:
        return None


def __read_csv_directory(inCsvFileName):
    callFailDataList = []
    absPath = os.path.abspath(inCsvFileName)
    print(absPath)
    for li in os.listdir(absPath):
        print(li)
        oldName = os.path.join(absPath, li)

        callFailData1 = __read_one_csv_file(oldName)
        if callFailData1 is not None:
            callFailDataList.append(callFailData1)

    callFailData = callFailDataList[0]
    for i in range(1, len(callFailDataList)):
        callFailData = callFailData.append(callFailDataList[i], ignore_index=True)

    print(callFailData.shape)
    return callFailData


def __process_zhejiang_IMEI(callFailData, path, file_pre, cs_ps):
    model_list_fp = open(os.path.join('.', 'config', '云诊断内销浙江统计机型列表.txt'), 'r')
    modelList = []
    for model in model_list_fp.readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, file_pre + '_数据分析结果_浙江IMEI' + cs_ps + '.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)

    # ---对每一个型号进行过滤和对比
    # 如果包含在写入excel表格
    list_result = []
    for model in modelList:
        model0 = model.split('_')[0]
        model1 = model.split('_')[1]

        worksheet = workbook.add_worksheet(model)
        worksheet.set_column('A:A', 20)

        before = str(callFailData.shape[0])
        callFailData_after = callFailData[callFailData['机型'] == model0]
        after = str(callFailData_after.shape[0])

        print('开始过滤' + model + '...' + after + '/' + before)

        # 获取dataframe中的所有IMEI数据
        imeiList_a = []
        for imei in callFailData_after['imei'].tolist():
            imeiList_a.append(str(imei).strip())

        # 获取文件中浙江的IMEI列表
        imeiList_b = []
        fileName = os.path.join('.', 'zhejiang_imei', model1 + '.txt')
        imeiFile_fp = open(fileName, 'r')
        imei_zhejiang = imeiFile_fp.readlines()
        for imei in imei_zhejiang:
            imeiList_b.append(imei.strip())

        # 获得浙江IMEI列表和dataframe IMEI中的交集
        IMEI_intersection = list(set(imeiList_a).intersection(set(imeiList_b)))
        # print('a='+str(len(imeiList_a))+',b='+str(len(imeiList_b))+',intersection='+str(len(IMEI_intersection)))

        # 按照dataframe的数量排序，获取浙江输出到excel
        callFailData_IMEI = callFailData_after['imei'].value_counts()
        allIMEI = callFailData_IMEI.index.tolist()

        row_i = 0
        for imei_i in range(len(allIMEI)):
            for imei_filtered in IMEI_intersection:
                if (imei_filtered == allIMEI[imei_i]):
                    worksheet.write(row_i, 0, imei_filtered)
                    worksheet.write(row_i, 1, callFailData_IMEI.values[imei_i])
                    list_result.append((imei_filtered, callFailData_IMEI.values[imei_i]), )
                    row_i += 1

    # ---对所有过滤出来的浙江IMEI计算Top
    print('ouput all...')
    worksheet = workbook.add_worksheet('all')
    worksheet.set_column('A:A', 20)
    mylist = sorted(list_result, key=lambda t: t[1], reverse=True)
    for i in range(len(mylist)):
        worksheet.write(i, 0, mylist[i][0])
        worksheet.write(i, 1, mylist[i][1])
    workbook.close()

    length_mylist = 0
    if (len(mylist) < 1):
        callFailData_internal = pd.DataFrame(columns=callFailData.columns)
    else:
        if (len(mylist) < 10):
            length_mylist = len(mylist)
        else:
            length_mylist = 10

        callFailDataList = []
        for i in range(length_mylist):
            callFailData_internal = callFailData[callFailData['imei'] == mylist[i][0]]
            callFailDataList.append(callFailData_internal)

        callFailData_internal = pd.DataFrame(columns=callFailData.columns)
        for i in range(1, len(callFailDataList)):
            callFailData_internal = callFailData_internal.append(callFailDataList[i], ignore_index=True)

    xls_fileName1 = os.path.join(path, file_pre + '_数据分析结果_浙江IMEI详细信息' + cs_ps + '.xlsx')
    writer = ExcelWriter(xls_fileName1)
    callFailData_internal.to_excel(writer, 'data')
    writer.save()


def __process_trial_IMEI(callFailData, path, inCsvFileName_head, cs_ps):
    modelList = []
    for model in open(os.path.join('.', 'config', '云诊断内销掉话试用机列表.txt'), 'r').readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, inCsvFileName_head + '_数据分析结果_试用机IMEI' + cs_ps + '.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)

    xls_fileName1 = os.path.join(path, inCsvFileName_head + '_数据分析结果_试用机IMEI详细信息' + cs_ps + '.xlsx')
    writer = ExcelWriter(xls_fileName1)

    # ---对每一个试用机机型进行过滤和比对
    for model in modelList:
        model0 = model.split('_')[0]
        model1 = model.split('_')[1]
        worksheet = workbook.add_worksheet(model)
        before = str(callFailData.shape[0])
        private_callFailData = callFailData[callFailData['机型'] == model0]
        after = str(private_callFailData.shape[0])

        print('开始过滤' + model + '...' + after + '/' + before)

        imeiList_a = []
        for imei in private_callFailData['imei'].tolist():
            imeiList_a.append(str(imei).strip())

        fileName = os.path.join('.', 'trial_imei', model1 + '.txt')
        imeiFile_fp = open(fileName, 'r')
        imeiList_b = []
        for imei in imeiFile_fp.readlines():
            imeiList_b.append(imei.split()[0].strip())
            imeiList_b.append(imei.split()[1].strip())

        IMEI_intersection = list(set(imeiList_a).intersection(set(imeiList_b)))
        print(
            'a=' + str(len(imeiList_a)) + ',b=' + str(len(imeiList_b)) + 'intersection=' + str(len(IMEI_intersection)))

        private_callFailData1 = pd.DataFrame(columns=callFailData.columns)
        for imei_i in range(len(IMEI_intersection)):
            worksheet.write(imei_i, 0, IMEI_intersection[imei_i])
            private_callFailData1 = private_callFailData1.append(
                private_callFailData[callFailData['imei'] == IMEI_intersection[imei_i]])

        private_callFailData1.to_excel(writer, model)
    writer.save()


def cloud_out_oos_main(path_raw_data, path_result):
    main_function('云诊断外销掉网', path_raw_data, path_result, __read_one_csv_file, __read_csv_directory,
                  __clean_data_all_data)


def cloud_out_oos_plot_trend(path_raw_data, path_result):
    sheet_name_list = ['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']

    trend_dics_list = {}

    trend_dics_list['掉网类型'] = ['CS', 'PS', 'CS+PS']

    trend_dics_list['地区码'] = ['泰国', '印度尼西亚', '印度', '菲律宾', '缅甸', '马来西亚', '越南', '老挝', '柬埔寨', '孟加拉国', '其他']

    trend_dics_list['出现异常的卡'] = ['卡1', '卡2']

    trend_dics_list['机型'] = ['PD1624F_EX', 'PD1613F_EX', 'PD1612F_EX', 'PD1612BF_EX', 'PD1708F_EX', 'PD1705F_EX']

    trend_dics_list['CS_NW'] = ['GSM-Unknown', 'LTE-Unknown', 'UMTS-Unknown', '1xRTT-Unknown',
                                'TD-SCDMA-Unknown', 'CDMA-IS95A-Unknown']

    trend_dics_list['PS_NW'] = ['EDGE-Unknown', 'LTE-Unknown', 'HSPA-Unknown', 'HSDPA-Unknown',
                                'GPRS-Unknown', 'UMTS-Unknown', 'HSPAP-Unknown', 'LTE_CA-Unknown']

    trend_dics_list['掉网时长'] = ['15', '3']

    trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    trend_dics_list['失败类型'] = ['CS', 'PS', 'CS_PS']

    trend_dics_list['失败原因1'] = ['0 Unspecified_failure', '21 REJECT_CAUSE_Synch_failure', '15 REJECT_CAUSE_No_suitable_cells_in_tracking_area', '11 REJECT_CAUSE_PLMN_not_allowed', '13 REJECT_CAUSE_Roaming_not_allowed_in_this_tracking_area']

    plot_trend('云诊断外销掉网', path_raw_data, path_result, trend_dics_list)


if __name__ == '__main__':
    path = os.path.abspath(
        'D:/tools/pycharm_projects/bigdata_analysis/cloud_out_oos_raw_data/cloud_out_oos_raw_data_weeks/test')
    cloud_out_oos_main(path, path)








