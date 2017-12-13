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
    df_csv_datas = df_csv_datas.drop(['emmcid','上报时间','异常进程名','进程版本名','进程版本号','异常进程包名',
        '软件系统类型','国家','异常类型','MBN版本信息','异常次数','日志路径'],axis=1)

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

    print('\t字段转义')
    df_csv_datas['机型'] = df_csv_datas['外部机型'].str.cat(df_csv_datas['内部机型'],sep='/')
    df_csv_datas = df_csv_datas.drop(['外部机型','内部机型'],axis=1)

    print('\t发生时间--提取发生的小时')
    df_csv_datas['发生时间1'] = pd.to_datetime(df_csv_datas['发生时间'],infer_datetime_format=True)
    df_csv_datas['发生时间h'] = df_csv_datas['发生时间1'].apply(get_hour)
    df_csv_datas = df_csv_datas.drop(['发生时间','发生时间1'],axis=1)

    df_csv_datas['ImsRat'] = df_csv_datas['保留字段一']
    df_csv_datas = df_csv_datas.drop(['保留字段一'],axis=1)

    df_csv_datas['ExtraCode_ErroMSG'] = df_csv_datas['保留字段二']
    df_csv_datas['ExtraCode'] = df_csv_datas['保留字段二'].apply(get_ExtraCode)
    df_csv_datas['ErroMSG'] = df_csv_datas['保留字段二'].apply(get_ErroMSG)
    df_csv_datas = df_csv_datas.drop(['保留字段二'],axis=1)

    df_csv_datas['RSRP'] = df_csv_datas['log信息'].apply(get_rsrp)
    df_csv_datas['RSRQ'] = df_csv_datas['log信息'].apply(get_rsrq)
    df_csv_datas = df_csv_datas.drop(['log信息'],axis=1)

    df_csv_datas['运营商1'] = df_csv_datas['运营商'].apply(get_plmn1)
    df_csv_datas['运营商2'] = df_csv_datas['运营商'].apply(get_plmn2)
    df_csv_datas = df_csv_datas.drop(['运营商'],axis=1)

    print('\t合并一些共同分析的字段')
    df_csv_datas['开始基站位置'] = df_csv_datas['运营商2'].str.cat(df_csv_datas['起呼位置码'],sep='/').str.cat(df_csv_datas['起呼基站编号'],sep='/')
    df_csv_datas['结束基站位置'] = df_csv_datas['运营商2'].str.cat(df_csv_datas['结束位置码'],sep='/').str.cat(df_csv_datas['结束基站编号'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼位置码','起呼基站编号'],axis=1)
    df_csv_datas = df_csv_datas.drop(['结束位置码','结束基站编号'],axis=1)

    df_csv_datas['电话网络']=df_csv_datas['起呼电话网络'].str.cat(df_csv_datas['结束电话网络'],sep='/')
    df_csv_datas['数据网络']=df_csv_datas['开始数据网络'].str.cat(df_csv_datas['结束数据网络'],sep='/')
    df_csv_datas['网络']=df_csv_datas['电话网络'].str.cat(df_csv_datas['数据网络'],sep='/')
    df_csv_datas = df_csv_datas.drop(['起呼电话网络','结束电话网络'],axis=1)
    df_csv_datas = df_csv_datas.drop(['开始数据网络','结束数据网络'],axis=1)

    df_csv_datas['运营商_电话网络'] = df_csv_datas['运营商2'].str.cat(df_csv_datas['电话网络'], sep='/')
    df_csv_datas['运营商_数据网络'] = df_csv_datas['运营商2'].str.cat(df_csv_datas['数据网络'], sep='/')
    df_csv_datas['运营商_网络'] = df_csv_datas['运营商2'].str.cat(df_csv_datas['网络'], sep='/')

    df_csv_datas['运营商_网络_原因'] = df_csv_datas['运营商_网络'].str.cat(df_csv_datas['ExtraCode'], sep='/')

    df_csv_datas['省市'] = df_csv_datas['省直辖市'].str.cat(df_csv_datas['市'],sep='/')
    df_csv_datas['省市县区'] = df_csv_datas['省市'].str.cat(df_csv_datas['县区'],sep='/')
    df_csv_datas = df_csv_datas.drop(['市'],axis=1)
    df_csv_datas = df_csv_datas.drop(['县区'],axis=1)

    row_length_after = df_csv_datas.shape[0]
    print('\t数据清洗前后的数量='+str(row_length_after)+'/'+str(row_length_before)+',数据清洗比率='+str(row_length_after*100/row_length_before)+'%')

    return df_csv_datas

def get_rsrp(name):
    min_rsrp = -1
    min_rsrq = -1
    if('RSRP' in name and 'RSRQ' in name and ';' in name):
        rsrp_rsrq_list = [rsrp_rsrq for rsrp_rsrq in name.split(';')]
        for item in rsrp_rsrq_list:
            rsrp_str = item.split(' ')[0][len('RSRP='):]
            rsrq_str = item.split(' ')[-1]
            try:
                rsrp = eval(rsrp_str)
                rsrq = eval(rsrq_str)
            except:
                rsrp = -1
                rsrq = -1
            if(rsrp < min_rsrp):
                min_rsrp = rsrp
            if(rsrq < min_rsrq):
                min_rsrq = rsrq
    return int(min_rsrp/5)*5

def get_rsrq(name):
    min_rsrp = -1
    min_rsrq = -1
    if('RSRP' in name and 'RSRQ' in name and ';' in name):
        rsrp_rsrq_list = [rsrp_rsrq for rsrp_rsrq in name.split(';')]
        for item in rsrp_rsrq_list:
            rsrp_str = item.split(' ')[0][len('RSRP='):]
            rsrq_str = item.split(' ')[-1]
            try:
                rsrp = eval(rsrp_str)
                rsrq = eval(rsrq_str)
            except:
                rsrp = -1
                rsrq = -1
            if(rsrp < min_rsrp):
                min_rsrp = rsrp
            if(rsrq < min_rsrq):
                min_rsrq = rsrq
    return int(min_rsrq)

def get_ImsRat(name):
    if(';' in name):
        return name.split(';')[0]
    else:
        return '-1'

def get_ExtraCode(name):
    if(';' in name):
        return name.split(';')[0]
    else:
        return '-1'

def get_ErroMSG(name):
    if(';' in name):
        return name.split(';')[1]
    else:
        return '-1'

def get_plmn1(name):
    try:
        plmn = ''.join(name.split(' ')[:-1])
        if plmn == '':
            return '-1'
        else:
            return plmn
        return
    except:
        return '-1'

def get_plmn2(name):
    try:
        plmn = name.split(' ')[-1]
        if plmn == '' or plmn == '-1':
            return '-1'
        else:
            return plmn
    except:
        return '-1'

def get_hour(name):
    returnName=name.to_pydatetime().hour
    return returnName

def cloud_in_ims_main(path_raw_data,path_result):
    main_function('云诊断内销IMS', path_raw_data, path_result, clean_data)

def cloud_in_ims_plot_trend(path_raw_data,path_result):
    sheet_name_list=['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']

    trend_dics_list={}
    trend_dics_list['出现异常的卡']=['卡1','卡2']
    trend_dics_list['ExtraCode']=['ExtraCode=0','ExtraCode=804','ExtraCode=404','ExtraCode=805','ExtraCode=408','ExtraCode=480','ExtraCode=403','ExtraCode=500','ExtraCode=401','ExtraCode=503','ExtraCode=400']
    trend_dics_list['失败原因']=['1000','5','2','4']
    trend_dics_list['isim支持情况']=['ISIM=0;DUT=1','ISIM=0;DUT=0','ISIM=1;DUT=1','ISIM=1;DUT=0']
    trend_dics_list['运营商2'] = ['46000','46001','46019','46011','46003']
    trend_dics_list['VOLTE配置信息']=['CFG=3;ON=1','CFG=1;ON=1','CFG=1;ON=0']
    trend_dics_list['机型']=['PD1710/X20Plus','PD1709/X20A','PD1708/Y79A','PD1635/X9sPlus','PD1616B/X9s','PD1610/Xplay6','PD1619/X9Plus','PD1624/X9i','PD1616/X9']
    trend_dics_list['IMEI频次'] = ['PD1710/X20Plus','PD1709/X20A','PD1708/Y79A','PD1635/X9sPlus','PD1616B/X9s','PD1610/Xplay6','PD1619/X9Plus','PD1624/X9i','PD1616/X9','PD1718/PD1718','PD1621B/PD1621B']
    trend_dics_list['省直辖市']=['广东省','河南省','甘肃省','江苏省','河北省','山西省','浙江省','新疆维吾尔自治区',
                             '广西壮族自治区','安徽省','山东省','福建省','湖南省','贵州省','陕西省','云南省',
                             '黑龙江省','四川省','吉林省','辽宁省','湖北省','内蒙古自治区','宁夏回族自治区',
                             '北京市','上海市','江西省','重庆市','青海省','海南省','天津市','西藏自治区']
    plot_trend('云诊断内销IMS', path_raw_data, path_result, trend_dics_list)

if __name__ == '__main__':
    path1=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_ims_rawdata/weeks')
    path2=os.path.abspath('/opt/vivo-home/bigdata_cloud_datas/cloud_in_ims_resultdata/weeks')
    cloud_in_callfail_main(path1,path2)

    path3=os.path.join('/opt/vivo-home/bigdata_cloud_datas/cloud_in_ims_resultdata', '云诊断内销IMS周趋势.xlsx')
    cloud_in_call_fail_plot_trend(path2,path3)
