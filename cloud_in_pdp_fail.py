#encoding=utf-8

import pandas as pd
import xlsxwriter
import os
import platform
from pandas import ExcelWriter
from util import main_function,plot_trend

def __read_one_csv_file(inCsvFileName):
    try:
        callFailData=pd.read_csv(inCsvFileName, 
                                     dtype={'呼叫对方号码': object,'运营商': object,
                                            'imei': object,'起呼位置码': object,
                                            '起呼基站编号': object,'结束位置码': object,
                                            '结束基站编号': object,'isim支持情况': object,
                                            '失败原因':object})
        #print(callFailData.columns)
        #print(callFailData.shape)
        return callFailData
    except:
        return None

def __read_csv_directory(inCsvFileName):
    callFailDataList=[]
    absPath=os.path.abspath(inCsvFileName)
    print(absPath)
    for li in os.listdir(absPath):
        print(li)
        sysstr = platform.system()
        #print('current OS is '+sysstr)
        if(sysstr =="Windows"):
            oldName=absPath+'\\'+li
        elif(sysstr == "Linux"):
            oldName=absPath+'/'+li
        else:
            oldName=absPath+'/'+li
        
        callFailData1=__read_one_csv_file(oldName)
        if callFailData1 is not None:
            callFailDataList.append(callFailData1)
    
    callFailData = callFailDataList[0]
    for i in range(1,len(callFailDataList)):
        callFailData = callFailData.append(callFailDataList[i], ignore_index=True)
        
    print(callFailData.shape)
    return callFailData

def __get_fail_cause(name):
    return name.split('=')[1]

def __clean_data_all_data(callFailData):
    rowLength_before=callFailData.shape[0]
    #---原始数据，只是填充null，无任何过滤    
    callFailData=callFailData.fillna('null')
    
    print('-----------------------------------'+str(callFailData.shape[0]))
    shape=callFailData.shape[0]
    #---移除测试的PLMN
    callFailData=callFailData[callFailData['运营商'].apply(lambda x: x != '99901')]
    callFailData=callFailData[callFailData['运营商'].apply(lambda x: x != '00000')]
    callFailData=callFailData[callFailData['运营商'].apply(lambda x: x != '00101')]
    callFailData=callFailData[callFailData['运营商'].apply(lambda x: x != '123456')]
    callFailData=callFailData[callFailData['运营商'].apply(lambda x: x != 'null')]
    
    #---起呼位置码 0、1
    callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 0)]
    callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 1)]
    callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '0')]
    callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '1')]
 
    #---起呼基站编号 0、1
    callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 0)]
    callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 1)]
    callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '0')]
    callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '1')]
    
    #---起呼电话网络 UNKNOWN
    callFailData=callFailData[callFailData['起呼电话网络'].apply(lambda x: x != 'UNKNOWN')]
    
    #---添加辅助分析项
    callFailData['PLMN_LAC1_CID1']=callFailData['运营商'].str.cat(callFailData['起呼位置码'],sep='/').str.cat(callFailData['起呼基站编号'],sep='/')
    callFailData['PLMN_LAC2_CID2']=callFailData['运营商'].str.cat(callFailData['结束位置码'],sep='/').str.cat(callFailData['结束基站编号'],sep='/')
    
    callFailData['PLMN_CS1']=callFailData['运营商'].str.cat(callFailData['起呼电话网络'],sep='/')
    callFailData['PLMN_PS1'] = callFailData['运营商'].str.cat(callFailData['开始数据网络'], sep='/')
    
    callFailData['省直辖市']=callFailData['省/直辖市']
    callFailData['县区']=callFailData['县/区']   
    callFailData['市1']=callFailData['省直辖市'].str.cat(callFailData['市'],sep='/')
    callFailData['县区1']=callFailData['市1'].str.cat(callFailData['县区'],sep='/')
    
    callFailData['通话状态']=callFailData['呼叫对方号码'].apply(__removeStateSpace)
    callFailData['信号强度']=callFailData['isim支持情况'].apply(__getRSRP)
    
    callFailData['发生时间t']=pd.to_datetime(callFailData['发生时间'],infer_datetime_format=True)
    callFailData['发生时间h']=callFailData['发生时间t'].apply(__getHour)

    callFailData['出现异常的卡']=callFailData['出现异常的卡'].apply(__replace_sim)
    
    callFailData['呼入呼出']=callFailData['呼入呼出'].apply(__get_fail_cause)
    callFailData['失败原因']=callFailData['失败原因'].str.cat(callFailData['呼入呼出'],sep='=')
    
    callFailData['PS_原因']=callFailData['开始数据网络'].str.cat(callFailData['失败原因'],sep='/')

    callFailData['机型']=callFailData['外部机型']

    print(callFailData.shape)
    
    #callFailData = callFailData[callFailData['机型'] == 'PD1635']
    
    #---drop没有利用价值的项
    callFailData=callFailData.drop(['外部机型','内部机型','emmcid','地区码','上报时间','异常进程名','进程版本名',
                                    '进程版本号','异常进程包名','软件系统类型','异常类型','isim支持情况',
                                    'MBN版本信息','VOLTE配置信息','呼叫对方号码','保留字段一','保留字段二',
                                    '异常次数','日志路径','log信息','省/直辖市','县/区','发生时间','市',
                                    '县区','发生时间t','呼入呼出','起呼位置码', '起呼基站编号','结束位置码', 
                                    '结束基站编号', '市1', '县区1', '是否volte', '起呼电话网络',
                                    'PLMN_CS1','发生时间h'],axis=1)
    
    rowLength_after=callFailData.shape[0]
    print('数据清洗之后...'+str(rowLength_after)+'/'+str(rowLength_before))
    return callFailData,callFailData,shape

def __replace_sim(sim):
    if(sim==0):
        return '卡1'
    elif(sim==1):
        return '卡2'
    else:
        return 'null'
    
def __getHour(name):
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

def cloud_in_pdpfail_main(path_raw_data,path_result):
    main_function('云诊断内销上网激活失败',path_raw_data, path_result, __read_one_csv_file, __read_csv_directory, __clean_data_all_data)

def cloud_in_pdpfail_plot_trend(path_raw_data,path_result):
    sheet_name_list=['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']

    trend_dics_list={}

    trend_dics_list['失败原因']=['4101=OEM_DCFAILCAUSE_5', '4103=OEM_DCFAILCAUSE_7', '4100=OEM_DCFAILCAUSE_4', '33=SERVICE_OPTION_NOT_SUBSCRIBED', '29=USER_AUTHENTICATION', '38=NETWORK_FAILURE', '-3=SIGNAL_LOST', '4104=OEM_DCFAILCAUSE_8',]

    trend_dics_list['出现异常的卡']=['卡1','卡2']

    trend_dics_list['运营商']=['46000','46001','46011','46003']

    trend_dics_list['开始数据网络']=['EDGE','HSPA','CDMA - 1xRTT','LTE','HSDPA','UMTS',
                               'GPRS','CDMA - eHRPD','CDMA - EvDo rev. A','HSPA+',
                               'UNKNOWN','TD_SCDMA','LTE_CA']

    trend_dics_list['机型']=['PD1635','PD1624','PD1616B','PD1619','PD1610','PD1616']

    trend_dics_list['系统版本']=['PD1616B_A_1.6.18', 'PD1616B_A_1.7.1', 'PD1616B_A_1.7.7', 'PD1616B_A_1.7.8', 'PD1616B_A_1.7.10', 'PD1616B_A_1.7.13', 'PD1616B_A_1.8.5', 'PD1616B_A_1.8.9']

    trend_dics_list['省直辖市']=['广东省','河南省','甘肃省','江苏省','河北省','山西省','浙江省','新疆维吾尔自治区',
                             '广西壮族自治区','安徽省','山东省','福建省','湖南省','贵州省','陕西省','云南省',
                             '黑龙江省','四川省','吉林省','辽宁省','湖北省','内蒙古自治区','宁夏回族自治区',
                             '北京市','上海市','江西省','重庆市','青海省','海南省','天津市','西藏自治区']

    #trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    plot_trend('云诊断内销上网激活失败',path_raw_data, path_result,trend_dics_list)


