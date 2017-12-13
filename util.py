#!/usr/bin/python3
# -*-- encoding=utf-8 --*-

import os
import pandas as pd
from pandas import ExcelWriter
from data_sheet_operation import write_data_into_excel_overall,write_data_into_excel_every_item
import xlsxwriter
import codecs
import json

def read_one_txt_file_callfail_out(inCsvFileName):
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

def read_csv_dir_callfail_out(inCsvFileName):
    callFailDataList = []
    file_path_PD1612 = os.path.join(inCsvFileName, 'PD1612_804_8041_通话失败收集.txt')
    file_path_PD1613 = os.path.join(inCsvFileName, 'PD1613_804_8041_通话失败收集.txt')
    file_path_PD1624 = os.path.join(inCsvFileName, 'PD1624_804_8041_通话失败收集.txt')

    callFailData1 = read_one_txt_file_callfail_out(file_path_PD1612)
    callFailData2 = read_one_txt_file_callfail_out(file_path_PD1613)
    callFailData3 = read_one_txt_file_callfail_out(file_path_PD1624)

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

    callFailData = pd.concat(callFailDataList)

    return callFailData

def read_one_txt_file_callfail_in(inCsvFileName):
    print('read...' + inCsvFileName)
    fp = open(inCsvFileName, 'r', encoding='ISO-8859-1')
    allLines = fp.readlines()
    fp.close()

    if (len(allLines) == 0):
        print('file is empty...')
        return None

    imeiList = []
    modelList = []
    systemVersionList = []
    timeStampList1 = []
    timeStampList2 = []
    timeStampList3 = []
    timeStampList4 = []
    callFailItemsJsonList = []
    keylist = []
    for currentLine in allLines:
        items = currentLine.split('\t')

        imeiList.append(items[0])
        modelList.append(items[1])
        systemVersionList.append(items[2])
        keylist.append(items[5])
        timeStampList1.append(items[6])
        timeStampList2.append(items[8])
        timeStampList3.append(items[10])
        timeStampList4.append(items[12])
        callFailItemsJsonList.append(items[14])

    df_csv_datasJson = pd.DataFrame.from_records(map(json.loads, callFailItemsJsonList))

    df_csv_datas_part1 = pd.DataFrame({'imei（default）': imeiList,
                                       '机型（default）': modelList,
                                       '系统版本（default）': systemVersionList,
                                       '主键（default）': keylist,
                                       '事件开始时间（default）': timeStampList1,
                                       '事件结束时间（default）': timeStampList2,
                                       '事件发生时间（default）': timeStampList3,
                                       '事件持续时间（default）': timeStampList4})

    df_csv_datas = df_csv_datas_part1.join(df_csv_datasJson)

    df_csv_datas['电话网络'] = df_csv_datas['cs']
    df_csv_datas['SIM卡'] = df_csv_datas['si']
    df_csv_datas['运营商'] = df_csv_datas['pl']
    df_csv_datas['基站编码'] = df_csv_datas['ci']
    df_csv_datas['位置码'] = df_csv_datas['la']
    df_csv_datas['呼入或呼出'] = df_csv_datas['io']
    df_csv_datas['数据网络'] = df_csv_datas['ps']
    df_csv_datas['失败原因'] = df_csv_datas['fa']
    df_csv_datas['地址'] = df_csv_datas['ad']
    df_csv_datas['预留字段1'] = df_csv_datas['re1']
    df_csv_datas['预留字段2'] = df_csv_datas['re2']

    # df_csv_datas = df_csv_datas.drop(['cs','si','pl','ci','ad','la','io','ps','fa','re2','re1'],axis=1)
    df_csv_datas = df_csv_datas[['SIM卡','失败原因','地址','呼入或呼出','位置码','基站编码','运营商','电话网络','数据网络','预留字段1','预留字段2','主键（default）','imei（default）','机型（default）','系统版本（default）','事件开始时间（default）','事件结束时间（default）','事件发生时间（default）','事件持续时间（default）']]

    return df_csv_datas

def read_one_csv_file_callfail_in(inCsvFileName):
    opertorName = codecs.decode(bytes('运营商', 'gbk'), encoding='ISO-8859-1')
    lacName = codecs.decode(bytes('位置码', 'gbk'), encoding='ISO-8859-1')
    cidName = codecs.decode(bytes('基站编码', 'gbk'), encoding='ISO-8859-1')
    csName = codecs.decode(bytes('电话网络', 'gbk'), encoding='ISO-8859-1')
    imeiName = codecs.decode(bytes('imei（default）', 'gbk'), encoding='ISO-8859-1')
    modelName = codecs.decode(bytes('机型（default）', 'gbk'), encoding='ISO-8859-1')
    versionName = codecs.decode(bytes('系统版本（default）', 'gbk'), encoding='ISO-8859-1')
    df_csv_datas = pd.read_csv(inCsvFileName, dtype=object, encoding="ISO-8859-1", error_bad_lines=False, low_memory=False)
    columnsList = []
    for i in df_csv_datas.columns:
        columnsList.append(codecs.decode(bytes(i, 'ISO-8859-1'), encoding="gbk"))
    df_csv_datas.columns = columnsList

    return df_csv_datas

def read_csv_dir_callfail_in(inCsvFileName):
    '''
    遍历目录下所有的csv文件
    '''
    df_csv_datas_list = []
    path_1 = os.path.abspath(inCsvFileName)
    print(path_1)
    for li in os.listdir(path_1):
        oldName = os.path.join(path_1, li)
        print(oldName)
        if(os.path.splitext(li)[1] == '.txt'):
            df_csv_datas1 = read_one_txt_file_callfail_in(oldName)
        elif(os.path.splitext(li)[1] == '.csv'):
            df_csv_datas1 = read_one_csv_file_callfail_in(oldName)
        df_csv_datas_list.append(df_csv_datas1)

    df_csv_datas = pd.concat(df_csv_datas_list)
    return df_csv_datas

def read_one_csv(csv_file_name):
    '''
    读取一个csv文件，dataframe结构
    '''
    try:
        df_csv_data = pd.read_csv(csv_file_name, dtype = str, low_memory = False)
        return df_csv_data
    except:
        return None

def read_csv_dir(csv_dir_path):
    '''
    读取一个目录下的所有的csv文件，并合并成一个dataframe结构
    '''
    df_csv_data_list = []
    csv_dir_abspath = os.path.abspath(csv_dir_path)
    print(csv_dir_abspath)
    for file in os.listdir(csv_dir_abspath):
        print(file)
        csv_file_name = os.path.join(csv_dir_abspath,file)
        df_csv_data_one = read_one_csv(csv_file_name)
        df_csv_data_list.append(df_csv_data_one)
    df_csv_datas = pd.concat(df_csv_data_list)

    print(df_csv_datas.shape)
    return df_csv_datas

def get_hour(name):
    returnName = name.to_pydatetime().hour
    return returnName

def get_mcc(name):
    res = '0'
    if(name != '-1' and name != ' ' and name != '' and len(name) > 3):
        try:
            res = name[:3]
        except:
            res = '0'
    return res

def get_mnc(name):
    res = '0'
    if(name != '-1' and name != ' ' and name != '' and len(name) > 3):
        try:
            res = name[3:].strip()
        except:
            res = '0'
    else:
        res = '0'
    return str(int(res))

def get_min_rsrp(name):
    name = name.strip()
    rsrp_list = []
    returnValue = -1
    if(name != '-1' and name != 'null' and name != '0' and ',' in name):
        rsrp_list = [eval(rsrp) for rsrp in name.split(',')[:-1]]
        min_rsrp = min(rsrp_list)
        if(min_rsrp != 0):
            returnValue = int(min_rsrp / 5) * 5
    return returnValue

def remove_douhao(name):
    if '.' in name:
        name = name.split('.')[0]
    return str(name)

def main_function(fail_type,raw_data_path,result_data_path,clean_data):
    result_path_list = [file_path for file_path in os.listdir(result_data_path)]

    df_csv_datas = None
    for file in os.listdir(raw_data_path):
        print(file)
        file_join = os.path.join(raw_data_path,file)
        file_pre = os.path.splitext(file)[0]
        filename_output_overall = file_pre + '_' + fail_type + '_' + '数据分析结果_整体.xlsx'
        filename_output_everyitem = file_pre + '_' + fail_type + '_' + '数据分析结果_Top分析.xlsx'
        filename_combined_csv = file_pre + '_' + fail_type + '_' + '合并后.csv'

        result_data_every_path = os.path.join(result_data_path,file_pre)

        if(fail_type == '大数据内销掉话'):
            if file_pre in result_path_list:
                print('\t'+file_pre + ' 已经存在，无需再处理.')
                continue
            else:
                os.mkdir(result_data_every_path)
                print(file_join)
                if os.path.isdir(file_join):
                    df_csv_datas = read_csv_dir_callfail_in(file_join)
                else:
                    continue
            if df_csv_datas is None:
                print('\t当前处理的文档是空的......')
                continue
                return
        elif(fail_type == '大数据外销掉话'):
            if file_pre in result_path_list:
                print('\t'+file_pre + ' 已经存在，无需再处理.')
                continue
            else:
                os.mkdir(result_data_every_path)
                print(file_join)
                if os.path.isdir(file_join):
                    df_csv_datas = read_csv_dir_callfail_out(file_join)
                else:
                    continue
            if df_csv_datas is None:
                print('\t当前处理的文档是空的......')
                continue
                return
        else:
            if file_pre in result_path_list:
                print('\t'+file_pre + ' 已经存在，无需再处理.')
                continue
            else:
                os.mkdir(result_data_every_path)
                print(file_join)
                if os.path.isdir(file_join):
                    df_csv_datas = read_csv_dir(file_join)
                elif os.path.isfile(file_join):
                    df_csv_datas = read_one_csv(file_join)
                else:
                    continue
            if df_csv_datas is None:
                print('\t当前处理的文档是空的......')
                continue
                return

        print('1.开始数据清洗')
        df_csv_datas = clean_data(df_csv_datas)

        if(fail_type.startswith('云诊断外销')):
            plmn_data1 = pd.read_csv('./config/mcc-mnc1.csv',dtype=str)
            df_csv_datas = pd.merge(df_csv_datas, plmn_data1, on=['地区码'], how='left')
            df_csv_datas = df_csv_datas.drop(['地区码'],axis=1)
            plmn_data2 = pd.read_csv('./config/mcc-mnc2.csv',dtype=str)
            df_csv_datas['运营商'] = df_csv_datas['运营商'].apply(remove_douhao)
            df_csv_datas['MCC'] = df_csv_datas['运营商'].apply(get_mcc)
            df_csv_datas['MNC'] = df_csv_datas['运营商'].apply(get_mnc)
            df_csv_datas = pd.merge(df_csv_datas, plmn_data2, on=['MCC','MNC'], how='left')
            df_csv_datas['运营商n'] = (df_csv_datas['运营商'].str.cat(df_csv_datas['国家2'],sep='/').str.cat(df_csv_datas['运营商2'],sep='/')).astype(str)
            df_csv_datas = df_csv_datas.drop(['MCC','MNC','国家2','运营商2'],axis=1)

        elif(fail_type.startswith('大数据外销')):
            plmn_data2 = pd.read_csv('./config/mcc-mnc2.csv',dtype=str)
            df_csv_datas['运营商'] = df_csv_datas['运营商'].apply(remove_douhao)
            df_csv_datas['MCC'] = df_csv_datas['运营商'].apply(get_mcc)
            df_csv_datas['MNC'] = df_csv_datas['运营商'].apply(get_mnc)
            df_csv_datas = pd.merge(df_csv_datas, plmn_data2, on=['MCC','MNC'], how='left')
            df_csv_datas['运营商n'] = (df_csv_datas['运营商'].str.cat(df_csv_datas['国家2'],sep='/').str.cat(df_csv_datas['运营商2'],sep='/')).astype(str)
            df_csv_datas = df_csv_datas.drop(['MCC','MNC', '运营商2'],axis=1)

        combined_csv_saved_path = os.path.join(result_data_path,file_pre,filename_combined_csv)
        df_csv_datas.to_csv(combined_csv_saved_path)

        fp_model_sample = open(os.path.join(os.path.abspath('.'),'config','sample.csv'))
        model_sample_list = fp_model_sample.readlines()
        fp_model_sample.close()

        temp_dir_path = os.path.join(result_data_path,file_pre,'temp')
        os.mkdir(temp_dir_path)

        print('\t正在导出 IMEI频次......')
        fp_imei_freq = open(os.path.join(temp_dir_path,file_pre +'_'+fail_type + '_' +'IMEI频次.csv'),'w')
        fp_imei_freq.write('IMEI频次,\n')
        for item in df_csv_datas['机型'].unique():
            model_imei_counter = df_csv_datas[df_csv_datas['机型'] == item]
            fp_imei_freq.write(item+','+str(model_imei_counter.shape[0]/len(model_imei_counter['imei'].unique()))+'\n')
        fp_imei_freq.close()

        if(not fail_type.endswith('RAT切换')):
            print('\t正在导出 失败原因频次......')
            fp_cause_freq = open(os.path.join(temp_dir_path,file_pre +'_'+fail_type + '_' +'失败原因频次.csv'),'w')
            fp_cause_freq.write('失败原因频次,\n')
            for item in df_csv_datas['失败原因'].unique():
                cause_imei_counter = df_csv_datas[df_csv_datas['失败原因'] == item]
                fp_cause_freq.write(item+','+str(cause_imei_counter.shape[0]/len(cause_imei_counter['imei'].unique()))+'\n')
            fp_cause_freq.close()

        if (not fail_type.startswith('云诊断外销')) and (not fail_type.startswith('大数据')):
            print('\t正在导出 省份频次......')
            fp_province_freq = open(os.path.join(temp_dir_path,file_pre +'_'+fail_type + '_' +'省份频次.csv'),'w')
            fp_province_freq.write('省份频次,\n')
            for item in df_csv_datas['省直辖市'].unique():
                province_imei_counter = df_csv_datas[df_csv_datas['省直辖市'] == item]
                fp_province_freq.write(item+','+str(province_imei_counter.shape[0]/len(province_imei_counter['imei'].unique()))+'\n')
            fp_province_freq.close()

        if(not fail_type.endswith('上网激活失败')) and (not fail_type.endswith('MMS')) and (not fail_type.endswith('PDP激活失败')):
            print('\t正在导出 网络频次1......')
            fp_net1_freq = open(os.path.join(temp_dir_path,file_pre +'_'+fail_type + '_' +'网络频次1.csv'),'w')
            fp_net1_freq.write('网络频次1,\n')
            for item in df_csv_datas['运营商_电话网络'].unique():
                net1_imei_counter = df_csv_datas[df_csv_datas['运营商_电话网络'] == item]
                fp_net1_freq.write(item+','+str(net1_imei_counter.shape[0]/len(net1_imei_counter['imei'].unique()))+'\n')
            fp_net1_freq.close()

        print('\t正在导出 网络频次2......')
        fp_net1_freq = open(os.path.join(temp_dir_path,file_pre +'_'+fail_type + '_' +'网络频次2.csv'),'w')
        fp_net1_freq.write('网络频次2,\n')
        for item in df_csv_datas['运营商_数据网络'].unique():
            net1_imei_counter = df_csv_datas[df_csv_datas['运营商_数据网络'] == item]
            fp_net1_freq.write(item+','+str(net1_imei_counter.shape[0]/len(net1_imei_counter['imei'].unique()))+'\n')
        fp_net1_freq.close()

        print('\t导出每一个字段的csv counter统计，为绘制趋势图准备.')
        for item in df_csv_datas.columns:
            counter_every_item = df_csv_datas[item].value_counts()
            counter_every_item.to_frame().to_csv(os.path.join(temp_dir_path,file_pre +'_'+fail_type+'_' + item + '.csv'),index=True)

        print('2.开始导出到整体 excel')
        write_data_into_excel_overall(df_csv_datas,os.path.join(result_data_path,file_pre,filename_output_overall))
        print('-----导出overall完成-----\n')

        if(fail_type.startswith('云诊断内销RAT切换') or fail_type.startswith('云诊断外销RAT切换')):
            pass
        else:
            print('3.开始导出到Top分析 excel-----')
            write_data_into_excel_every_item(df_csv_datas,os.path.join(result_data_path,file_pre,filename_output_everyitem))
            print('-----导出every_item完成-----\n')

def plot_trend(class_name,path_raw_data,path_result,trend_dics_list):
    result_file_list = []
    file_list = os.listdir(path_raw_data)
    for file_i in range(len(file_list)):
        result_file_list.append(file_list[file_i])
    result_file_list.sort()

    fp3 = open(os.path.join(os.path.abspath('.'),'config','sample.csv'))
    all_model_sample = fp3.readlines()
    fp3.close()

    workbook_dst = xlsxwriter.Workbook(path_result)
    for sheet_name,value_list in trend_dics_list.items():
        chart = workbook_dst.add_chart({'type': 'line'})
        chart.set_drop_lines({'line': {'color': 'red','dash_type': 'square_dot'}})

        sheet_dst = workbook_dst.add_worksheet(sheet_name)
        sheet_dst.write(0,0,sheet_name)
        print('当前正在绘制--' + sheet_name + '...')
        for file_i in range(len(result_file_list)):
            print(result_file_list[file_i])
            result_filename = os.path.join(path_raw_data,result_file_list[file_i],'temp',result_file_list[file_i]+'_'+class_name+'_'+sheet_name + '.csv')
            sheet_dst.write(file_i + 1,0,result_file_list[file_i])
            with open(result_filename,'r',encoding='utf-8') as workbook_src:
                for line in workbook_src.readlines():
                    for value_i in range(len(value_list)):
                        sheet_dst.write(0,value_i + 1,value_list[value_i])
                        if line.split(',')[0] == value_list[value_i]:
                            flag = 0
                            if(sheet_name == '机型'):
                                for i in all_model_sample:
                                    if i.split(',')[0] == line.split(',')[0]:
                                        sheet_dst.write(file_i + 1,value_i + 1,float(line.split(',')[1])/float(i.split(',')[1])*1000)
                                        flag = 1
                                if flag == 0:
                                    sheet_dst.write(file_i + 1,value_i + 1,float(line.split(',')[1]))
                            else:
                                sheet_dst.write(file_i + 1, value_i + 1, float(line.split(',')[1]))

        for item_i in range(len(value_list)):
            chart.add_series({
                'name': [sheet_name,0,item_i + 1],
                'categories': [sheet_name,1,0,len(result_file_list),0],
                'values': [sheet_name,1,item_i + 1,len(result_file_list),item_i + 1],
                'marker': {'type': 'automatic'},
                'data_labels': {'value': True},
            })
            chart.set_x_axis({'name': '时间'})
            chart.set_y_axis({'name': '出现异常次数'})
            chart.set_title({'name': sheet_name})
            chart.set_size({'width': 1400,'height': 700})
        sheet_dst.insert_chart('A' + str(4 + len(result_file_list)),chart)
    workbook_dst.close()
