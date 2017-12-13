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
    df_csv_datas = df_csv_datas.drop(['emmcid','上报时间','异常进程名','进程版本名',
                                    '进程版本号','异常进程包名','软件系统类型','异常类型','MBN版本信息','国家',
                                    'VOLTE配置信息','保留字段一','保留字段二','异常次数','日志路径','结束电话网络',
                                    '结束数据网络','isim支持情况','MBN版本信息','是否volte','呼叫对方号码','结束基站编号',
                                    'log信息'],axis=1)

    df_csv_datas = df_csv_datas.fillna('-1')

    print('\t地区码--只保留中国')
    df_csv_datas = df_csv_datas.loc[df_csv_datas['地区码'] == 'china']
    df_csv_datas = df_csv_datas.drop(['地区码'],axis=1)

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

    print('\t处理一些处理异常的字段')
    df_csv_datas['省直辖市'] = df_csv_datas['省/直辖市']
    df_csv_datas = df_csv_datas.drop(['省/直辖市'],axis=1)

    df_csv_datas['县区'] = df_csv_datas['县/区']
    df_csv_datas = df_csv_datas.drop(['县/区'],axis=1)

    df_csv_datas['电话号码'] = df_csv_datas['结束位置码']
    df_csv_datas = df_csv_datas.drop(['结束位置码'],axis=1)

    print('\t合并一些共同分析的字段')
    df_csv_datas['开始基站位置'] = df_csv_datas['运营商'].str.cat(df_csv_datas['起呼位置码'],sep='/').str.cat(df_csv_datas['起呼基站编号'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼位置码','起呼基站编号'],axis=1)

    df_csv_datas['运营商_电话网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['起呼电话网络'], sep='/')
    df_csv_datas['运营商_数据网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['开始数据网络'], sep='/')

    df_csv_datas['省市'] = df_csv_datas['省直辖市'].str.cat(df_csv_datas['市'],sep='/')
    df_csv_datas['省市县区'] = df_csv_datas['省市'].str.cat(df_csv_datas['县区'],sep='/')
    df_csv_datas = df_csv_datas.drop(['市'],axis=1)
    df_csv_datas = df_csv_datas.drop(['县区'],axis=1)

    print('\t发生时间--提取发生的小时')
    df_csv_datas['发生时间1'] = pd.to_datetime(df_csv_datas['发生时间'],infer_datetime_format=True)
    df_csv_datas['发生时间h'] = df_csv_datas['发生时间1'].apply(get_hour)
    df_csv_datas = df_csv_datas.drop(['发生时间','发生时间1'],axis=1)

    df_csv_datas['机型'] = df_csv_datas['外部机型'].str.cat(df_csv_datas['内部机型'],sep='/')
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

def cloud_in_sms_main(path_raw_data,path_result):
    main_function('云诊断内销SMS', path_raw_data, path_result, clean_data)

def cloud_in_sms_plot_trend(path_raw_data,path_result):
    trend_dics_list={}
    trend_dics_list['出现异常的卡']=['卡1','卡2']
    trend_dics_list['失败原因']=['8','4','38','21','50','127','28','105','5','10','30','69','41','97','96','3','0','1','95','98','1003','100']
    trend_dics_list['运营商']=['46000','46001','46019','46003','46011']
    trend_dics_list['起呼电话网络'] = ['LTE','GSM','UNKNOWN','UMTS','EDGE','HSPA','CDMA - 1xRTT','CDMA - eHRPD','CDMA - EvDo rev. A','GPRS','TD_SCDMA','LTE_CA','HSDPA','HSPA+','CDMA']
    trend_dics_list['开始数据网络'] = ['LTE','GSM','UNKNOWN','UMTS','EDGE','HSPA','CDMA - 1xRTT','CDMA - eHRPD','CDMA - EvDo rev. A','GPRS','TD_SCDMA','LTE_CA','HSDPA','HSPA+','CDMA']
    trend_dics_list['机型']=['PD1710/X20Plus','PD1709/X20A','PD1708/Y79A','PD1635/X9sPlus','PD1616B/X9s','PD1610/Xplay6','PD1619/X9Plus','PD1624/X9i','PD1616/X9']
    trend_dics_list['IMEI频次'] = ['PD1710/X20Plus','PD1709/X20A','PD1708/Y79A','PD1635/X9sPlus','PD1616B/X9s','PD1610/Xplay6','PD1619/X9Plus','PD1624/X9i','PD1616/X9','PD1718/PD1718','PD1621B/PD1621B']
    trend_dics_list['省直辖市']=['广东省','河南省','甘肃省','江苏省','河北省','山西省','浙江省','新疆维吾尔自治区',
                             '广西壮族自治区','安徽省','山东省','福建省','湖南省','贵州省','陕西省','云南省',
                             '黑龙江省','四川省','吉林省','辽宁省','湖北省','内蒙古自治区','宁夏回族自治区',
                             '北京市','上海市','江西省','重庆市','青海省','海南省','天津市','西藏自治区']
    plot_trend('云诊断内销SMS', path_raw_data, path_result, trend_dics_list)

if __name__ == '__main__':
    path1=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_rawdata/weeks')
    path2=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_reportdata/weeks')
    cloud_in_sms_main(path1,path2)

    path3=os.path.join('/opt/vivo-home/bigdata_cloud_datas/cloud_in_sms_reportdata', '云诊断内销SMS周趋势.xlsx')
    cloud_in_sms_plot_trend(path2,path3)
