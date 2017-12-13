#encoding=utf-8
import pandas as pd
import xlsxwriter
import os
import platform
from pandas import ExcelWriter
from util import main_function,plot_trend,get_min_rsrp

def clean_data(df_csv_datas):
    '''
    数据清洗
    '''
    row_length_before = df_csv_datas.shape[0]

    print('\t清理--清除一些用不到的字段')
    df_csv_datas = df_csv_datas.drop(['emmcid','上报时间','异常进程名','进程版本名','进程版本号','异常进程包名','失败原因',
        '软件系统类型','国家','省/直辖市','市','县/区','详细地址','异常类型','呼入呼出','isim支持情况','MBN版本信息',
        'VOLTE配置信息','是否volte','保留字段一','保留字段二','异常次数','日志路径','log信息'],axis=1)

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

    print('\t处理一些处理异常的字段')

    df_csv_datas['机型'] = df_csv_datas['外部机型'].str.cat(df_csv_datas['内部机型'],sep='/')
    df_csv_datas = df_csv_datas.drop(['外部机型','内部机型'],axis=1)

    print('\t合并一些共同分析的字段')
    df_csv_datas['开始基站位置'] = df_csv_datas['运营商'].str.cat(df_csv_datas['起呼位置码'],sep='/').str.cat(df_csv_datas['起呼基站编号'],sep='/')
    df_csv_datas['结束基站位置'] = df_csv_datas['运营商'].str.cat(df_csv_datas['结束位置码'],sep='/').str.cat(df_csv_datas['结束基站编号'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼位置码','起呼基站编号'],axis=1)
    df_csv_datas = df_csv_datas.drop(['结束位置码','结束基站编号'],axis=1)

    df_csv_datas['电话网络']=df_csv_datas['起呼电话网络'].str.cat(df_csv_datas['结束电话网络'],sep='/')
    df_csv_datas['数据网络']=df_csv_datas['开始数据网络'].str.cat(df_csv_datas['结束数据网络'],sep='/')
    df_csv_datas['网络']=df_csv_datas['电话网络'].str.cat(df_csv_datas['数据网络'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼电话网络','结束电话网络'],axis=1)
    df_csv_datas = df_csv_datas.drop(['开始数据网络','结束数据网络'],axis=1)

    df_csv_datas['运营商_电话网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['电话网络'], sep='/')
    df_csv_datas['运营商_数据网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['数据网络'], sep='/')
    df_csv_datas['运营商_网络'] = df_csv_datas['运营商'].str.cat(df_csv_datas['网络'], sep='/')

    print('\t发生时间--提取发生的小时')
    df_csv_datas['发生时间1'] = pd.to_datetime(df_csv_datas['发生时间'],infer_datetime_format=True,errors='ignore')
    df_csv_datas['发生时间h'] = df_csv_datas['发生时间1'].apply(get_hour)
    df_csv_datas = df_csv_datas.drop(['发生时间','发生时间1'],axis=1)

    print('\t出现异常的卡--替换sim卡的定义')
    df_csv_datas['出现异常的卡'] = df_csv_datas['出现异常的卡'].apply(replace_sim)

    df_csv_datas['信号强度'] = df_csv_datas['呼叫对方号码'].apply(get_min_rsrp)
    df_csv_datas = df_csv_datas.drop(['呼叫对方号码'],axis=1)

    row_length_after = df_csv_datas.shape[0]
    print('\t数据清洗前后的数量='+str(row_length_after)+'/'+str(row_length_before)+',数据清洗比率='+str(row_length_after*100/row_length_before)+'%')

    return df_csv_datas

def get_fail_cause(name):
    if '=' in name:
        return name.split('=')[1]
    else:
        return '-1'

def replace_sim(sim):
    if(sim=='0'):
        return '卡1'
    elif(sim=='1'):
        return '卡2'
    else:
        return '-1'

def get_hour(name):
    if type(name) == str:
        return -1
    else:
        returnName=name.to_pydatetime().hour
        return returnName

def __getRSRP(name):
    returnName=name.strip()
    rsrp_list=[]
    returnValue=0
    if(name=='-1' or name=='null'):
        returnValue=str(-1)
    else:
        rsrp_list=returnName.split(',')[-2]
        returnValue=int(eval(rsrp_list)/10)*10
    return returnValue

def __removeCauseID(name):
    returnName=name.strip()
    if(name.startswith('CALL_END_CAUSE_UNSPECIFIED')):
        returnName='CALL_END_CAUSE_UNSPECIFIED'
    elif(name.startswith('ERROR_UNSPECIFIED')):
        returnName='ERROR_UNSPECIFIED'
    else:
        pass
    return returnName

def __removeCauseNormal(name):
    returnName=name.strip()
    if(name.endswith('_NORMAL')):
        returnName=returnName[:-len('_NORMAL')]
    else:
        pass
    return returnName

def __removeStateSpace(name):
    returnName=name.strip()
    if(' ' in name):
        returnName=','.join(name.split(' '))
    else:
        pass
    return returnName

def cloud_out_ratchange_main(path_raw_data,path_result):
    main_function('云诊断外销RAT切换',path_raw_data, path_result, clean_data)

def cloud_out_ratchange_plot_trend(path_raw_data,path_result):
    trend_dics_list={}
    trend_dics_list['国家1'] = ['Philippines','India','Indonesia','Myanmar (Burma)','Thailand','Malaysia','Pakistan','Viet Nam']
    trend_dics_list['出现异常的卡'] = ['卡1', '卡2']
    trend_dics_list['机型']=['PD1705F_EX','PD1708F_EX','PD1624F_EX','PD1718F_EX']
    trend_dics_list['IMEI频次'] = ['PD1705F_EX','PD1708F_EX','PD1624F_EX','PD1718F_EX']

    plot_trend('云诊断外销RAT切换',path_raw_data, path_result,trend_dics_list)
