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
                                            '结束基站编号': object,'isim支持情况': object},low_memory=False)
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

def __clean_data_all_data(callFailData):
    #'内部机型', '外部机型', '系统版本', 'emmcid', 'imei', '地区码', '发生时间', '上报时间', '异常进程名', '进程版本名',
    # '进程版本号', '异常进程包名', '软件系统类型', '国家', '省/直辖市', '市', '县/区', '详细地址', '异常类型', '出现异常的卡',
    # '失败原因', '呼入呼出', '起呼位置码', '起呼基站编号', '起呼电话网络', '开始数据网络', '运营商', '结束位置码',
    # '结束基站编号', '结束电话网络', '结束数据网络', 'isim支持情况', 'MBN版本信息', 'VOLTE配置信息', '是否volte',
    # '呼叫对方号码', '保留字段一', '保留字段二', '异常次数', '日志路径', 'log信息'

    rowLength_before=callFailData.shape[0]
    #---原始数据，只是填充null，无任何过滤    
    callFailData=callFailData.fillna('null')
    
    callFailData = callFailData.loc[(callFailData["是否volte"] != "CS")]
    
    #---只是过滤掉正常的原因（网络释放原因）    
    fp=open(os.path.join('.','config','remove_items.txt'),'r')
    allines=fp.readlines()
    for cause in allines:
        callFailData=callFailData[callFailData['失败原因'].apply(lambda x: x!=cause.strip())]
        
    print('-----------------------------------'+str(callFailData.shape[0]))
    
    shape_after_remove_cause=callFailData.shape[0]
    #---移除测试的PLMN
    callFailData = callFailData.loc[(callFailData["运营商"] != "99901") &
                                    (callFailData["运营商"] != "00000") &
                                    (callFailData["运营商"] != "00101") &
                                    (callFailData["运营商"] != "123456") &
                                    (callFailData["运营商"] != "null")]
 
#     #---起呼位置码 0、1
#     callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 0)]
#     callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != 1)]
#     callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '0')]
#     callFailData=callFailData[callFailData['起呼位置码'].apply(lambda x: x.strip() != '1')]
#  
#     #---结束位置码 0、1
#     callFailData=callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != 0)]
#     callFailData=callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != 1)]
#     callFailData=callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != '0')]
#     callFailData=callFailData[callFailData['结束位置码'].apply(lambda x: x.strip() != '1')]
#      
#     #---起呼基站编号 0、1
#     callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 0)]
#     callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != 1)]
#     callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '0')]
#     callFailData=callFailData[callFailData['起呼基站编号'].apply(lambda x: x.strip() != '1')]
#      
#     #---结束基站编号 0、1
#     callFailData=callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != 0)]
#     callFailData=callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != 1)]
#     callFailData=callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != '0')]
#     callFailData=callFailData[callFailData['结束基站编号'].apply(lambda x: x.strip() != '1')]
#      
#     #---起呼电话网络 UNKNOWN
#     callFailData=callFailData[callFailData['起呼电话网络'].apply(lambda x: x != 'UNKNOWN')]
 
    callFailData = callFailData.loc[(callFailData["imei"] != "123456789012345")]
    
    callFailData['出现异常的卡']=callFailData['出现异常的卡'].apply(__replace_sim)
    
    callFailData = callFailData.loc[(callFailData["运营商"] == "46000")|(callFailData["运营商"] == "46002")|(callFailData["运营商"] == "46007")]
    callFailData = callFailData.loc[(callFailData["外部机型"] == "PD1635")|(callFailData["外部机型"] == "PD1616B")]
    
    imei_list = callFailData["imei"].tolist()
    imei_list1 = list(set(imei_list))
    
    imei_df = pd.DataFrame(imei_list1,columns=['imei'])
    
    writer = ExcelWriter('201708.xlsx')
    imei_df.to_excel(writer, 'data', index=False)
    writer.save()
    
    print('=====================================')

    #---添加辅助分析项
    callFailData['PLMN_LAC1_CID1']=callFailData['运营商'].str.cat(callFailData['起呼位置码'],sep='/').str.cat(callFailData['起呼基站编号'],sep='/')
    callFailData['PLMN_LAC2_CID2']=callFailData['运营商'].str.cat(callFailData['结束位置码'],sep='/').str.cat(callFailData['结束基站编号'],sep='/')

    callFailData['CS_NW']=callFailData['起呼电话网络'].str.cat(callFailData['结束电话网络'],sep='/')
    callFailData['PS_NW']=callFailData['开始数据网络'].str.cat(callFailData['结束数据网络'],sep='/')
    callFailData['CS_PS_NW']=callFailData['CS_NW'].str.cat(callFailData['PS_NW'],sep='/')

    callFailData['PLMN_CS1'] = callFailData['运营商'].str.cat(callFailData['起呼电话网络'], sep='/')
    
    callFailData['PLMN_CS_NW'] = callFailData['运营商'].str.cat(callFailData['CS_NW'], sep='/')
    callFailData['PLMN_PS_NW'] = callFailData['运营商'].str.cat(callFailData['PS_NW'], sep='/')
    callFailData['PLMN_CS_PS_NW'] = callFailData['运营商'].str.cat(callFailData['CS_PS_NW'], sep='/')
    
    callFailData['机型-版本'] = callFailData['外部机型'].str.cat(callFailData['系统版本'], sep='/')
    
    callFailData['省直辖市']=callFailData['省/直辖市']
    callFailData['县区']=callFailData['县/区']   
    callFailData['市1']=callFailData['省直辖市'].str.cat(callFailData['市'],sep='-')
    callFailData['县区1']=callFailData['市1'].str.cat(callFailData['县区'],sep='-')
    
    callFailData['通话状态']=callFailData['呼叫对方号码'].apply(__removeStateSpace)
    callFailData['信号强度']=callFailData['isim支持情况'].apply(__getRSRP)
    
    callFailData['发生时间t']=pd.to_datetime(callFailData['发生时间'],infer_datetime_format=True)
    callFailData['发生时间h']=callFailData['发生时间t'].apply(__getHour)

    callFailData['机型']=callFailData['外部机型']
    
    #PD1635    PD1616B    PD1619    PD1624    PD1616
    #callFailData = callFailData[callFailData['机型'] == 'PD1616']
    #callFailData = callFailData[callFailData['失败原因'] == 'CALL_END_CAUSE_FADE_V02']

    callFailData['通话类型'] = callFailData['CS_NW'].str.cat(callFailData['是否volte'], sep='/')
    callFailData['通话类型1'] = callFailData['PLMN_CS_NW'].str.cat(callFailData['是否volte'], sep='/')
    callFailData['cause-state'] = callFailData['失败原因'].str.cat(callFailData['通话状态'], sep='/')
    callFailData['CS_sig'] = callFailData['通话类型'].str.cat(callFailData['信号强度'], sep='/')
    callFailData['cause_cs_sig'] = callFailData['失败原因'].str.cat(callFailData['CS_sig'], sep='/')
    
    #---drop没有利用价值的项
    data_every_file1=callFailData.drop(['外部机型','内部机型','emmcid','地区码','上报时间','异常进程名','进程版本名',
                                    '进程版本号','异常进程包名','软件系统类型','异常类型','isim支持情况',
                                    'MBN版本信息','VOLTE配置信息','呼叫对方号码','保留字段一','保留字段二',
                                    '异常次数','日志路径','log信息','省/直辖市','县/区','发生时间','市',
                                    '县区','发生时间t','机型-版本','起呼位置码','结束位置码','起呼基站编号','结束基站编号',
                                    '结束电话网络','结束数据网络','PS_NW','CS_PS_NW',
                                    'PLMN_PS_NW','PLMN_CS_PS_NW','发生时间h','市1','县区1',
                                    ],axis=1)
    
    rowLength_after=callFailData.shape[0]
    print('数据清洗之后...'+str(rowLength_after)+'/'+str(rowLength_before))
    return data_every_file1,data_every_file1,shape_after_remove_cause

def __get_mcc(name):
    return(name[:3])

def __replace_sim(sim):
    if(sim==1):
        return '卡1'
    elif(sim==2):
        return '卡2'
    else:
        return 'null'
    
def __getHour(name):
    returnName=name.to_pydatetime().hour
    return returnName
    
def __getRSRP(name):
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

def __removeStateSpace(name):
    returnName=name.strip()
    if(' ' in name):
        returnName=','.join(name.split(' '))
    else:
        pass    
    return returnName

def __process_zhejiang_IMEI(callFailData,path,file_pre):
    model_list_fp=open(os.path.join('.','config','云诊断内销浙江统计机型列表.txt'),'r')
    modelList=[]
    for model in model_list_fp.readlines():
        modelList.append(model.strip())
    
    xls_fileName=os.path.join(path,file_pre+'_数据分析结果_浙江IMEI.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)
    
    #---对每一个型号进行过滤和对比
    #如果包含在写入excel表格
    list_result=[]
    for model in modelList:
        model0=model.split('_')[0]
        model1=model.split('_')[1]
        
        worksheet = workbook.add_worksheet(model)
        worksheet.set_column('A:A',20)
        
        before=str(callFailData.shape[0])
        callFailData_after=callFailData[callFailData['外部机型']==model0]
        after=str(callFailData_after.shape[0])
        
        print('开始过滤'+model+'...'+after+'/'+before)
        
        #获取dataframe中的所有IMEI数据
        imeiList_a=[]
        for imei in callFailData_after['imei'].tolist():
            imeiList_a.append(str(imei).strip())

        #获取文件中浙江的IMEI列表
        imeiList_b=[]
        fileName=os.path.join('.','zhejiang_imei',model1+'.txt')
        imeiFile_fp=open(fileName,'r')
        imei_zhejiang=imeiFile_fp.readlines()
        for imei in imei_zhejiang:
            imeiList_b.append(imei.strip())
        
        #获得浙江IMEI列表和dataframe IMEI中的交集
        IMEI_intersection=list(set(imeiList_a).intersection(set(imeiList_b)))
        #print('a='+str(len(imeiList_a))+',b='+str(len(imeiList_b))+',intersection='+str(len(IMEI_intersection)))
        
        #按照dataframe的数量排序，获取浙江输出到excel
        callFailData_IMEI=callFailData_after['imei'].value_counts()
        allIMEI=callFailData_IMEI.index.tolist()
        
        row_i=0
        for imei_i in range(len(allIMEI)):            
            for imei_filtered in IMEI_intersection:
                if(imei_filtered==allIMEI[imei_i]):
                    
                    worksheet.write(row_i,0,imei_filtered)
                    worksheet.write(row_i,1,callFailData_IMEI.values[imei_i])
                    list_result.append((imei_filtered,callFailData_IMEI.values[imei_i]),)
                    row_i += 1
    
    #---对所有过滤出来的浙江IMEI计算Top
    print('ouput all...')
    worksheet = workbook.add_worksheet('all') 
    worksheet.set_column('A:A',20)
    mylist=sorted(list_result,key=lambda t:t[1],reverse=True)  
    for i in range(len(mylist)):
        worksheet.write(i,0,mylist[i][0])
        worksheet.write(i,1,mylist[i][1])
    workbook.close()
    
    length_mylist=0
    if(len(mylist) < 1):
        callFailData_internal = pd.DataFrame(columns=callFailData.columns)   
    else:
        if(len(mylist) < 10):
            length_mylist=len(mylist)
        else:
            length_mylist=10
            
        callFailDataList=[]
        for i in range(length_mylist):
            callFailData_internal=callFailData[callFailData['imei']==mylist[i][0]]
            callFailDataList.append(callFailData_internal)
            
        callFailData_internal = pd.DataFrame(columns=callFailData.columns)
        for i in range(1,len(callFailDataList)):
            callFailData_internal = callFailData_internal.append(callFailDataList[i], ignore_index=True)

    xls_fileName1=os.path.join(path,file_pre+'_数据分析结果_浙江IMEI详细信息.xlsx')
    writer = ExcelWriter(xls_fileName1)
    callFailData_internal.to_excel(writer,'data')
    writer.save()

def __process_trial_IMEI(callFailData,path,inCsvFileName_head):        
    modelList=[]
    for model in open(os.path.join('.','config','云诊断内销掉话试用机列表.txt'),'r').readlines():
        modelList.append(model.strip())
    
    xls_fileName=os.path.join(path,inCsvFileName_head+'_数据分析结果_试用机IMEI.xls')
    workbook = xlsxwriter.Workbook(xls_fileName)
    
    xls_fileName1=os.path.join(path,inCsvFileName_head+'_数据分析结果_试用机IMEI详细信息.xlsx')
    writer = ExcelWriter(xls_fileName1)
    
    #---对每一个试用机机型进行过滤和比对
    for model in modelList:
        model0=model.split('_')[0]
        model1=model.split('_')[1]
        worksheet = workbook.add_worksheet(model)
        before=str(callFailData.shape[0])
        private_callFailData=callFailData[callFailData['外部机型']==model0]
        after=str(private_callFailData.shape[0])
        
        print('开始过滤'+model+'...'+after+'/'+before)
        
        imeiList_a=[]
        for imei in private_callFailData['imei'].tolist():
            imeiList_a.append(str(imei).strip())
        
        fileName=os.path.join('.','trial_imei',model1+'.txt')
        imeiFile_fp=open(fileName,'r')
        imeiList_b=[]
        for imei in imeiFile_fp.readlines():
            imeiList_b.append(imei.split()[0].strip())
            imeiList_b.append(imei.split()[1].strip())
        
        IMEI_intersection=list(set(imeiList_a).intersection(set(imeiList_b)))
        print('a='+str(len(imeiList_a))+',b='+str(len(imeiList_b))+'intersection='+str(len(IMEI_intersection)))
        
        private_callFailData1=pd.DataFrame(columns=callFailData.columns)
        for imei_i in range(len(IMEI_intersection)):
            worksheet.write(imei_i,0,IMEI_intersection[imei_i])
            private_callFailData1=private_callFailData1.append(private_callFailData[callFailData['imei']==IMEI_intersection[imei_i]])

        private_callFailData1.to_excel(writer,model)
    writer.save()

def cloud_in_callfail_main(path_raw_data,path_result):
    main_function('云诊断内销掉话', path_raw_data, path_result, __read_one_csv_file, __read_csv_directory,
                  __clean_data_all_data)

def cloud_in_call_fail_plot_trend(path_raw_data,path_result):
    sheet_name_list=['SIM卡', '失败原因', '呼入或呼出', '运营商', '电话网络', '发生时间h', '机型', '系统版本', 'PLMN_CS']
    
    trend_dics_list={}
    trend_dics_list['出现异常的卡']=['卡1','卡2']
     
    trend_dics_list['通话类型1'] = ['46000/GSM/GSM/CS', '46001/UMTS/UMTS/CS', 
                                '46000/LTE/GSM/CS', '46000/LTE/LTE/VOLTE', 
                                '46011/CDMA - 1xRTT/CDMA - 1xRTT/CS']
  
    trend_dics_list['失败原因']=['CALL_END_CAUSE_RECOVERY_ON_TIMER_EXPIRED_V02', 'CALL_END_CAUSE_FADE_V02', 'CALL_END_CAUSE_RADIO_LINK_LOST_V02', 'CALL_END_CAUSE_UNSPECIFIED_16', 'CALL_END_CAUSE_REQUEST_TERMINATED_V02']
  
    trend_dics_list['呼入呼出']=['In','Out']
    
    trend_dics_list['运营商']=['46000','46001','46011','46003']
    
    trend_dics_list['是否volte']=['CS','VOLTE','VILTE']
    
    trend_dics_list['系统版本']=['PD1616B_A_1.6.18', 'PD1616B_A_1.7.1', 'PD1616B_A_1.7.7', 'PD1616B_A_1.7.8', 'PD1616B_A_1.7.10', 'PD1616B_A_1.7.13', 'PD1616B_A_1.8.5', 'PD1616B_A_1.8.9']
 
    trend_dics_list['机型']=['PD1635', 'PD1616B', 'PD1619', 'PD1624', 'PD1616']
# 
    trend_dics_list['CS_NW']=['GSM/GSM','UMTS/UMTS','LTE/LTE','LTE/GSM','LTE/UMTS']
  
    trend_dics_list['省直辖市']=['广东省','河南省','甘肃省','江苏省','河北省','山西省','浙江省','新疆维吾尔自治区',
                             '广西壮族自治区','安徽省','山东省','福建省','湖南省','贵州省','陕西省','云南省',
                             '黑龙江省','四川省','吉林省','辽宁省','湖北省','内蒙古自治区','宁夏回族自治区',
                             '北京市','上海市','江西省','重庆市','青海省','海南省','天津市','西藏自治区',
                             '香港特别行政区','澳门特别行政区','台湾省']
  
    trend_dics_list['移除正常cause之后大小'] = ['移除正常cause之后大小']

    plot_trend('云诊断内销掉话', path_raw_data, path_result, trend_dics_list)

if __name__ == '__main__':
    path=os.path.abspath('D:/tools/pycharm_projects/bigdata_analysis/cloud_in_callfail_raw_data/cloud_in_callfail_raw_data_weeks/test')
    cloud_in_callfail_main(path,path)








    