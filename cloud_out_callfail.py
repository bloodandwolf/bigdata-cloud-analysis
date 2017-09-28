# encoding=utf-8

import pandas as pd
import xlsxwriter
import os
import platform
from os.path import join
from pandas import ExcelWriter
from util import main_function, plot_trend


def __read_one_csv_file(inCsvFileName):
    callFailData = pd.DataFrame(columns=['内部机型', '外部机型', '系统版本', 'emmcid',
                                         'imei', '地区码', '发生时间', '上报时间',
                                         '异常进程名', '进程版本名', '进程版本号',
                                         '异常进程包名', '软件系统类型', '国家',
                                         '省/直辖市', '市', '县/区', '详细地址',
                                         '异常类型', '出现异常的卡', '失败原因',
                                         '呼入呼出', '起呼位置码', '起呼基站编号',
                                         '起呼电话网络', '开始数据网络', '运营商',
                                         '结束位置码', '结束基站编号', '结束电话网络',
                                         '结束数据网络', 'isim支持情况', 'MBN版本信息',
                                         'VOLTE配置信息', '是否volte', '呼叫对方号码',
                                         '保留字段一', '保留字段二', '异常次数', '日志路径',
                                         'log信息'])
    try:
        callFailData1 = pd.read_csv(inCsvFileName,
                                    dtype={'呼叫对方号码': object, '运营商': object,
                                           'imei': object, '起呼位置码': object,
                                           '起呼基站编号': object, '结束位置码': object,
                                           '结束基站编号': object, 'isim支持情况': object}, low_memory=False)
    except:
        callFailData1 = None

    if (callFailData1 is not None):
        callFailData = callFailData.append(callFailData1, ignore_index=True)
    else:
        callFailData_temp = pd.DataFrame(columns=['SIM卡', '失败原因', '地址', '呼入或呼出', '位置码', '基站编码',
                                                  '运营商', '电话网络', '数据网络', '预留字段1', '预留字段2',
                                                  '主键（default）', 'imei（default）', '机型（default）',
                                                  '系统版本（default）', '事件开始时间（default）',
                                                  '事件结束时间（default）', '事件发生时间（default）',
                                                  '事件持续时间（default）'])
        callFailData = callFailData.append(callFailData_temp, ignore_index=True)

    return callFailData


def __read_csv_directory(inCsvFileName):
    callFailDataList = []
    absPath = os.path.abspath(inCsvFileName)
    print(absPath)
    for li in os.listdir(absPath):
        print(li)
        sysstr = platform.system()
        # print('current OS is '+sysstr)
        if (sysstr == "Windows"):
            oldName = absPath + '\\' + li
        elif (sysstr == "Linux"):
            oldName = absPath + '/' + li
        else:
            oldName = absPath + '/' + li

        callFailData1 = __read_one_csv_file(oldName)
        if callFailData1 is not None:
            callFailDataList.append(callFailData1)

    callFailData = callFailDataList[0]
    for i in range(1, len(callFailDataList)):
        callFailData = callFailData.append(callFailDataList[i], ignore_index=True)

    print(callFailData.shape)
    return callFailData

def __clean_data_all_data(callFailData):
    rowLength_before = callFailData.shape[0]
    # ---原始数据，只是填充null，无任何过滤
    callFailData = callFailData.fillna('null')
    # ---只是过滤掉正常的原因（网络释放原因）
    fp = open(os.path.join('.', 'config', 'remove_items.txt'), 'r')
    allines = fp.readlines()
    for cause in allines:
        callFailData = callFailData[callFailData['失败原因'].apply(lambda x: x != cause.strip())]

    print('-----------------------------------' + str(callFailData.shape[0]))
    shape_remove_normal_cause = callFailData.shape[0]
    # ---移除测试的PLMN
    callFailData = callFailData[callFailData['运营商'].apply(lambda x: x != '99901')]
    callFailData = callFailData[callFailData['运营商'].apply(lambda x: x != '00000')]
    callFailData = callFailData[callFailData['运营商'].apply(lambda x: x != '00101')]
    callFailData = callFailData[callFailData['运营商'].apply(lambda x: x != '123456')]
    callFailData = callFailData[callFailData['运营商'].apply(lambda x: x != 'null')]

    # ---起呼位置码 0、1
    callFailData = callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 0)]
    callFailData = callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 1)]
    callFailData = callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '0')]
    callFailData = callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '1')]

    # ---结束位置码 0、1
    callFailData = callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != 0)]
    callFailData = callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != 1)]
    callFailData = callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != '0')]
    callFailData = callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != '1')]

    # ---起呼基站编号 0、1
    callFailData = callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 0)]
    callFailData = callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 1)]
    callFailData = callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '0')]
    callFailData = callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '1')]

    # ---结束基站编号 0、1
    callFailData = callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != 0)]
    callFailData = callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != 1)]
    callFailData = callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != '0')]
    callFailData = callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != '1')]

    # ---起呼电话网络 UNKNOWN
    callFailData = callFailData[callFailData['起呼电话网络'].apply(lambda x: x != 'UNKNOWN')]

    # ---添加辅助分析项
    callFailData['PLMN_LAC1_CID1'] = callFailData['运营商'].str.cat(callFailData['起呼位置码'], sep='/').str.cat(
        callFailData['起呼基站编号'], sep='/')
    callFailData['PLMN_LAC2_CID2'] = callFailData['运营商'].str.cat(callFailData['结束位置码'], sep='/').str.cat(
        callFailData['结束基站编号'], sep='/')

    callFailData['PLMN_CS1'] = callFailData['运营商'].str.cat(callFailData['起呼电话网络'], sep='/')
    callFailData['PLMN_CS2'] = callFailData['运营商'].str.cat(callFailData['结束电话网络'], sep='/')
    callFailData['CS_NW'] = callFailData['起呼电话网络'].str.cat(callFailData['结束电话网络'], sep='/')
    callFailData['PS_NW'] = callFailData['开始数据网络'].str.cat(callFailData['结束数据网络'], sep='/')
    callFailData['CS_PS_NW'] = callFailData['CS_NW'].str.cat(callFailData['PS_NW'], sep='/')

    callFailData['PLMN_CS_NW'] = callFailData['运营商'].str.cat(callFailData['CS_NW'], sep='/')
    callFailData['PLMN_PS_NW'] = callFailData['运营商'].str.cat(callFailData['PS_NW'], sep='/')
    callFailData['PLMN_CS_PS_NW'] = callFailData['运营商'].str.cat(callFailData['CS_PS_NW'], sep='/')

    callFailData['通话状态'] = callFailData['呼叫对方号码'].apply(__removeStateSpace)
    callFailData['信号强度'] = callFailData['isim支持情况'].apply(__getRSRP)

    callFailData['发生时间t'] = pd.to_datetime(callFailData['发生时间'], infer_datetime_format=True)
    callFailData['发生时间h'] = callFailData['发生时间t'].apply(__getHour)

    callFailData['出现异常的卡'] = callFailData['出现异常的卡'].apply(__replace_sim)

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
    callFailData.loc[callFailData['地区码'] == 'hk', '地区码'] = '香港'
    
    callFailData['通话类型'] = callFailData['CS_NW'].str.cat(callFailData['是否volte'], sep='/')
    callFailData['通话类型1'] = callFailData['PLMN_CS_NW'].str.cat(callFailData['是否volte'], sep='/')
    callFailData['cause-state'] = callFailData['失败原因'].str.cat(callFailData['通话状态'], sep='/')
    callFailData['CS_sig'] = callFailData['通话类型'].str.cat(callFailData['信号强度'], sep='/')
    callFailData['cause_cs_sig'] = callFailData['失败原因'].str.cat(callFailData['CS_sig'], sep='/')
    
    callFailData['地区机型'] = callFailData['地区码'].str.cat(callFailData['机型'], sep='/')

    # ---drop没有利用价值的项
    data_every_file1=callFailData.drop(['外部机型','内部机型','emmcid', '上报时间','异常进程名','进程版本名',
                                    '进程版本号','异常进程包名','软件系统类型','异常类型','isim支持情况',
                                    'MBN版本信息','VOLTE配置信息','呼叫对方号码','保留字段一','保留字段二',
                                    '异常次数','日志路径','log信息','省/直辖市','县/区','发生时间','市',
                                    '发生时间t', '起呼位置码','结束位置码','起呼基站编号','结束基站编号',
                                    '起呼电话网络','开始数据网络','结束电话网络','结束数据网络','PS_NW','CS_PS_NW',
                                    'PLMN_PS_NW','PLMN_CS_PS_NW','发生时间h','国家','详细地址',
                                    ],axis=1)

    rowLength_after = callFailData.shape[0]
    print('数据清洗之后...' + str(rowLength_after) + '/' + str(rowLength_before))
    return data_every_file1, data_every_file1, shape_remove_normal_cause


def __get_mcc(name):
    return (name[:3])


def __replace_sim(sim):
    if (sim == 1):
        return '卡1'
    elif (sim == 2):
        return '卡2'
    else:
        return 'null'


def __getHour(name):
    returnName = name.to_pydatetime().hour
    return returnName


def __getRSRP(name):
    returnName = name.strip()
    rsrp_list = []
    returnValue = 0
    if (name == '-1' or name == 'null'):
        returnValue = str(-1)
    else:
        rsrp_list = returnName.split(',')[-2]
        returnValue = int(eval(rsrp_list) / 10) * 10

    return str(returnValue)


def __removeCauseID(name):
    returnName = name.strip()
    if (name.startswith('CALL_END_CAUSE_UNSPECIFIED')):
        returnName = 'CALL_END_CAUSE_UNSPECIFIED'
    elif (name.startswith('ERROR_UNSPECIFIED')):
        returnName = 'ERROR_UNSPECIFIED'
    else:
        pass

    return returnName


def __removeCauseNormal(name):
    returnName = name.strip()
    if (name.endswith('_NORMAL')):
        returnName = returnName[:-len('_NORMAL')]
    else:
        pass

    return returnName


def __removeStateSpace(name):
    returnName = name.strip()
    if (' ' in name):
        returnName = ','.join(name.split(' '))
    else:
        pass

    return returnName


def __process_zhejiang_IMEI(callFailData, path, file_pre):
    model_list_fp = open(os.path.join('.', 'config', '云诊断内销浙江统计机型列表.txt'), 'r')
    modelList = []
    for model in model_list_fp.readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, file_pre + '_数据分析结果_浙江IMEI.xls')
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
        callFailData_after = callFailData[callFailData['外部机型'] == model0]
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

    xls_fileName1 = os.path.join(path, file_pre + '_数据分析结果_浙江IMEI详细信息.xlsx')
    writer = ExcelWriter(xls_fileName1)
    callFailData_internal.to_excel(writer, 'data')
    writer.save()


def __process_imei_zhejiang(callFailData):
    modelList = ['vivo X9', 'vivo X9i', 'vivo Xplay6', 'vivo Y55A', 'vivo Y67', 'vivo Y66']

    for model in modelList:
        fileName = join('.', 'zhejiang', model + '.txt')
        imeiFile_fp = open(fileName, 'r')

        callfaildata_list = []
        for imei in imeiFile_fp.readlines():
            mycallFailData = callFailData.copy()
            mycallFailData[callFailData['imei'] != imei.strip()] = 0
            mycallFailData = mycallFailData[mycallFailData['imei'].apply(lambda x: x != 0)]
            callfaildata_list.append(mycallFailData)

    mydata = callfaildata_list[0]
    for i in range(1, len(callfaildata_list)):
        mydata = mydata.append(callfaildata_list[i])

    cell_location = mydata['imei']
    top5_cell_location = cell_location.value_counts()
    print(top5_cell_location.index.tolist())
    print(top5_cell_location.values.tolist())


def __process_trial_IMEI(callFailData, path, inCsvFileName_head):
    modelList = []
    for model in open(os.path.join('.', 'config', '云诊断内销掉话试用机列表.txt'), 'r').readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, inCsvFileName_head + '_数据分析结果_试用机IMEI.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)

    xls_fileName1 = os.path.join(path, inCsvFileName_head + '_数据分析结果_试用机IMEI详细信息.xlsx')
    writer = ExcelWriter(xls_fileName1)

    # ---对每一个试用机机型进行过滤和比对
    for model in modelList:
        model0 = model.split('_')[0]
        model1 = model.split('_')[1]
        worksheet = workbook.add_worksheet(model)
        before = str(callFailData.shape[0])
        private_callFailData = callFailData[callFailData['外部机型'] == model0]
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


def cloud_out_callfail_main(path_raw_data, path_result):
    main_function('云诊断外销掉话', path_raw_data, path_result, __read_one_csv_file, __read_csv_directory,
                  __clean_data_all_data)


def cloud_out_call_fail_plot_trend(path_raw_data, path_result):
    sheet_name_list = ['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']

    trend_dics_list = {}
    trend_dics_list['出现异常的卡'] = ['卡1', '卡2']
    
    trend_dics_list['通话类型'] = ['HSPA/HSPA/CS', 'UMTS/UMTS/CS', 'GSM/GSM/CS', 'LTE/HSPA/CS', 'LTE/LTE/VOLTE']

    trend_dics_list['失败原因'] = ['ERROR_UNSPECIFIED_31', 'CALL_END_CAUSE_RECOVERY_ON_TIMER_EXPIRED_V02', 'CM_MM_RR_CONNECTION_RELEASE_2165', 'ERROR_UNSPECIFIED_2174', 'CALL_END_CAUSE_RLF_DURING_CC_DISCONNECT_V02', 'SERVICE_NOT_AVAILABLE_63', 'TIMED_OUT_18', 'DESTINATION_OUT_OF_ORDER_27', 'NO_USER_RESPONDING_18', 'CALL_END_CAUSE_RADIO_LINK_LOST_V02', 'CALL_END_CAUSE_FADE_V02']

    trend_dics_list['地区码'] = ['印度', '菲律宾', '泰国', '越南', '印度尼西亚', '马来西亚', '巴基斯坦', '缅甸', '柬埔寨']

    trend_dics_list['呼入呼出'] = ['In', 'Out']

    trend_dics_list['是否volte'] = ['CS', 'VOLTE', 'VILTE']

    trend_dics_list['机型'] = ['PD1612DF_EX', 'PD1628F_EX', 'PD1612BF_EX', 'PD1613BF_EX',
                             'PD1624F_EX', 'PD1705F_EX', 'PD1612F_EX']
    

    trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    plot_trend('云诊断外销掉话', path_raw_data, path_result, trend_dics_list)


if __name__ == '__main__':
    path = os.path.abspath(
        'D:/tools/pycharm_projects/bigdata_analysis/cloud_out_callfail_raw_data/cloud_out_callfail_raw_data_weeks/test')
    cloud_out_callfail_main(path, path)







