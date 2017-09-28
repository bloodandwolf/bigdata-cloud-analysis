#!/usr/bin/python3
# -*-- encoding=utf-8 --*-

import os
import pandas as pd
from pandas import ExcelWriter
from data_sheet_operation import write_data_into_excel_overall,write_data_into_excel_every_item
import xlsxwriter

def main_function(pre_name,path_raw_data, path_result,__read_one_csv_file,__read_csv_directory,__clean_data_all_data):
    path_weeks_file_list = []
    for file in os.listdir(path_result):
        path_weeks_file_list.append(file)
        print(file)

    # 判断目录下的所有文件是由已经分析过了，如果分析过了直接跳过，否则读取文件进行分析处理
    # path_raw_data=D:\tools\pycharm_projects\bigdata_analysis\big_data_in_raw_data\big_data_in_raw_data_weeks
    for file in os.listdir(path_raw_data):
        # file=20170702-20170708.csv
        file_join = os.path.join(path_raw_data, file)
        print(file)
        file_pre = os.path.splitext(file)[0]
        file_name_output_overall = file_pre + '_' + pre_name + '_' + '数据分析结果_整体.xlsx'
        file_name_output_every_item = file_pre + '_' + pre_name + '_' + '数据分析结果_Top分析.xlsx'

        if pre_name == '大数据外销掉话':
            if os.path.isdir(file_join):
                file_pre = os.path.splitext(file)[0]
                file_name_output_overall = file_pre + '_' + pre_name + '_' + '数据分析结果_整体.xlsx'
                file_name_output_every_item = file_pre + '_' + pre_name + '_' + '数据分析结果_Top分析.xlsx'
            else:
                continue

        data_every_file=None
        if file_pre in path_weeks_file_list:
            print(file_pre + ' 已经存在，无需再处理')
            pass
        else:
            os.mkdir(os.path.join(path_result, file_pre))
            if os.path.isfile(file_join):
                if pre_name == '大数据外销掉话':
                    pass
                else:
                    data_every_file = __read_one_csv_file(file_join)
            elif os.path.isdir(file_join):
                data_every_file = __read_csv_directory(file_join)

            if data_every_file is None:
                print('None')
                continue

            data_every_file, data_every_file2, shape_after_remove_cause = __clean_data_all_data(data_every_file)

            fp = open(os.path.join(path_result, file_pre, file_pre +'_'+pre_name + '_' +'移除正常cause之后大小.csv'), 'w')
            fp.write('移除正常cause之后大小,'+str(shape_after_remove_cause))
            fp.close()

            counter_every_item = data_every_file2['imei'].value_counts()

            counter_every_item2 = pd.DataFrame.from_dict(
                {'imei': counter_every_item.index.tolist(), 'counter': counter_every_item.tolist()})
            counter_every_item3 = counter_every_item2.head(20)

            data_every_file_remove_duplicate = data_every_file2.drop_duplicates('imei')
            imei_top20_filter = data_every_file_remove_duplicate.loc[
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[0]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[1]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[2]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[3]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[4]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[5]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[6]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[7]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[8]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[9]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[10]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[11]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[12]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[13]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[14]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[15]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[16]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[17]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[18]) |
                (data_every_file_remove_duplicate["imei"] == counter_every_item.index[19])]

            filename = os.path.join(path_result, file_pre, file_pre +'_'+pre_name+'_' +'回访.xlsx')
            print(filename)
            writer = ExcelWriter(filename)
            counter_every_item3.to_excel(writer, 'Top IMEI', index=False)
            imei_top20_filter.to_excel(writer, 'details', index=False)
            writer.save()

            for item in data_every_file.columns:
                counter_every_item = data_every_file[item].value_counts()
                counter_every_item.to_frame().to_csv(os.path.join(path_result, file_pre,file_pre +'_'+pre_name+'_' + item + '.csv'),index=True)

            data_every_file = data_every_file.drop(['imei','详细地址'],axis=1)
            print('-----开始导出到overall excel-----')
            write_data_into_excel_overall(data_every_file,
                                          os.path.join(path_result, file_pre, file_name_output_overall))
            print('-----导出overall完成-----\n')

            #print('-----开始导出到every_item excel-----')
            #write_data_into_excel_every_item(data_every_file,
            #                                 os.path.join(path_result, file_pre, file_name_output_every_item))
            #print('-----导出every_item完成-----\n')
            
            #print('-----开始导出到  浙江IMEI excel-----')
            #__process_zhejiang_IMEI(data_every_file, path_result, file_pre,pre_name)
            #print('-----导出完成-----\n')
            
            #print('-----开始导出到  试用机IMEI excel-----')
            #__process_trial_IMEI(data_every_file, path_result, file_pre,pre_name)
            #print('-----导出完成-----\n')

def plot_trend(class_name,path_raw_data, path_result,trend_dics_list):
    result_file_list = []
    file_list = os.listdir(path_raw_data)
    for file_i in range(len(file_list)):
        result_file_list.append(file_list[file_i])

    workbook_dst = xlsxwriter.Workbook(path_result)
    for sheet_name, value_list in trend_dics_list.items():
        chart = workbook_dst.add_chart({'type': 'line'})
        chart.set_drop_lines({'line': {'color': 'red', 'dash_type': 'square_dot'}})

        sheet_dst = workbook_dst.add_worksheet(sheet_name)
        sheet_dst.write(0, 0, sheet_name)
        print('当前正在处理--' + sheet_name + '...')
        for file_i in range(len(result_file_list)):
            print(result_file_list[file_i])
            result_filename = os.path.join(path_raw_data, result_file_list[file_i], result_file_list[file_i]+'_'+class_name+'_'+sheet_name + '.csv')
            #workbook_src = open(, 'r')
            sheet_dst.write(file_i + 1, 0, result_file_list[file_i])
            with open(result_filename, 'r',encoding='utf-8') as workbook_src:
                for line in workbook_src.readlines():
                    for value_i in range(len(value_list)):
                        sheet_dst.write(0, value_i + 1, value_list[value_i])
                        if line.split(',')[0] == value_list[value_i]:
                            sheet_dst.write(file_i + 1, value_i + 1, int(line.split(',')[1]))

        for item_i in range(len(value_list)):
            chart.add_series({
                'name': [sheet_name, 0, item_i + 1],
                'categories': [sheet_name, 1, 0, len(result_file_list), 0],
                'values': [sheet_name, 1, item_i + 1, len(result_file_list), item_i + 1],
                'marker': {'type': 'automatic'},
                'data_labels': {'value': True},
            })
            chart.set_x_axis({'name': '时间'})
            chart.set_y_axis({'name': '出现异常次数'})
            chart.set_title({ 'name': sheet_name})
        sheet_dst.insert_chart('A' + str(4 + len(result_file_list)), chart)
    workbook_dst.close()

def __process_zhejiang_IMEI(callFailData, path, file_pre,pre_name):
    model_list_fp = open(os.path.join('.', '..','bigdata_datas','config', '云诊断内销浙江统计机型列表.txt'), 'r')
    modelList = []
    for model in model_list_fp.readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, file_pre, file_pre +'_'+pre_name+'_' +'数据分析结果_浙江IMEI.xlsx')
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
        fileName = os.path.join('.', '..','bigdata_datas', 'zhejiang_imei', model1 + '.txt')
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

    xls_fileName1 = os.path.join(path, file_pre, file_pre +'_'+pre_name+'_' +'数据分析结果_浙江IMEI详细信息.xlsx')
    writer = ExcelWriter(xls_fileName1)
    callFailData_internal.to_excel(writer, 'data')
    writer.save()

def __process_trial_IMEI(callFailData, path, file_pre,pre_name):
    modelList = []
    for model in open(os.path.join('.','..','bigdata_datas', 'config', '云诊断内销掉话试用机列表.txt'), 'r').readlines():
        modelList.append(model.strip())

    xls_fileName = os.path.join(path, file_pre, file_pre +'_'+pre_name+'_' +'数据分析结果_试用机IMEI.xlsx')
    workbook = xlsxwriter.Workbook(xls_fileName)

    xls_fileName1 = os.path.join(path, file_pre, file_pre +'_'+pre_name+'_' +'数据分析结果_试用机IMEI详细信息.xlsx')
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

        fileName = os.path.join('.', '..','bigdata_datas', 'trial_imei', model1 + '.txt')
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
