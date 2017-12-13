# -*-- encoding=utf-8 --*-

import pandas as pd
import xlsxwriter
import os
import platform
from pandas import ExcelWriter
from util import main_function,plot_trend

def clean_data(df_csv_datas):
    '''
    数据清洗
    '''
    row_length_before = df_csv_datas.shape[0]

    print('\t清理--清除一些用不到的字段')
    df_csv_datas = df_csv_datas.drop(['emmcid','上报时间','异常进程名','进程版本名','进程版本号',
        '异常进程包名','软件系统类型','国家','省/直辖市','市','县/区','详细地址','异常类型','结束基站编号',
        '结束电话网络','结束数据网络','isim支持情况','MBN版本信息','VOLTE配置信息','是否volte','呼叫对方号码','保留字段一',
        '保留字段二','异常次数','日志路径','log信息'],axis=1)

    df_csv_datas = df_csv_datas.fillna('-1')

    print('\t运营商--移除测试的PLMN')
    fp = open(os.path.join(os.path.abspath('.'),'config','remove_test_plmn.txt'),'r')
    test_plmn_list = [plmn.strip() for plmn in fp.readlines()]
    df_csv_datas = df_csv_datas[-df_csv_datas['运营商'].isin(test_plmn_list)]
    fp.close()

    print('\tIMEI--移除测试的IMEI')
    fp = open(os.path.join(os.path.abspath('.'),'config','remove_test_imei.txt'),'r')
    test_imei_list = [imei.strip() for imei in fp.readlines()]
    df_csv_datas = df_csv_datas[-df_csv_datas['imei'].isin(test_imei_list)]
    fp.close()

    df_csv_datas['电话号码'] = df_csv_datas['结束位置码']
    df_csv_datas = df_csv_datas.drop(['结束位置码'],axis=1)

    print('\t合并一些共同分析的字段')
    df_csv_datas['开始基站位置'] = df_csv_datas['运营商'].str.cat(df_csv_datas['起呼位置码'],sep='/').str.cat(df_csv_datas['起呼基站编号'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼位置码','起呼基站编号'],axis=1)

    df_csv_datas['运营商_电话网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['起呼电话网络'], sep='/')
    df_csv_datas['运营商_数据网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['开始数据网络'], sep='/')

    print('\t发生时间--提取发生的小时')
    df_csv_datas['发生时间1'] = pd.to_datetime(df_csv_datas['发生时间'],infer_datetime_format=True)
    df_csv_datas['发生时间h'] = df_csv_datas['发生时间1'].apply(get_hour)
    df_csv_datas = df_csv_datas.drop(['发生时间','发生时间1'],axis=1)

    df_csv_datas['机型'] = df_csv_datas['外部机型']
    df_csv_datas = df_csv_datas.drop(['外部机型','内部机型'],axis=1)

    print('\t出现异常的卡--替换sim卡的定义')
    df_csv_datas['出现异常的卡'] = df_csv_datas['出现异常的卡'].apply(replace_sim)

    row_length_after = df_csv_datas.shape[0]
    print('\t数据清洗前后的数量='+str(row_length_after)+'/'+str(row_length_before)+',数据清洗比率='+str(row_length_after*100/row_length_before)+'%')

    return df_csv_datas

def replace_sim(sim):
    if(sim==0):
        return '卡1'
    elif(sim==1):
        return '卡2'
    else:
        return '-1'

def get_hour(name):
    returnName=name.to_pydatetime().hour
    return returnName

def cloud_out_sms_main(path_raw_data,path_result):
    main_function('云诊断外销SMS', path_raw_data, path_result, clean_data)

def cloud_out_sms_plot_trend(path_raw_data,path_result):
    trend_dics_list={}

    trend_dics_list['失败原因']=['0','21','4','38','29','5','50','69','10','42','96','8','31','41','28']
    # trend_dics_list['运营商2']=['46000','46001','46019','46003','46011']
    trend_dics_list['起呼电话网络'] = ['LTE','HSPA','UMTS','GSM','EDGE','GPRS','HSPA+','HSDPA']
    trend_dics_list['开始数据网络'] = ['LTE','HSPA','UMTS','GSM','EDGE','GPRS','HSPA+','HSDPA']
    trend_dics_list['机型']=['PD1708F','PD1705F','PD1705F_EX','PD1708F_EX','PD1624F_EX','PD1624F','PD1718F_EX']
    trend_dics_list['IMEI频次'] = ['PD1708F','PD1705F','PD1705F_EX','PD1708F_EX','PD1624F_EX','PD1624F','PD1718F_EX']
    trend_dics_list['国家1'] = ['Philippines','India','Indonesia','Myanmar (Burma)','Thailand','Malaysia','Pakistan','Viet Nam']

    # plot_trend('云诊断外销SMS', path_raw_data, path_result, trend_dics_list)

if __name__ == '__main__':
    path1=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_rawdata/weeks')
    path2=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_reportdata/weeks')
    cloud_in_sms_main(path1,path2)

    path3=os.path.join('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_reportdata', '云诊断内销SMS周趋势.xlsx')
    cloud_in_sms_plot_trend(path2,path3)
