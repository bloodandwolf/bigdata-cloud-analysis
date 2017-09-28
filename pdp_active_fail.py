#encoding=utf-8

import pandas as pd
import os
from data_sheet_operation import write_data_into_excel_overall,\
    write_data_into_excel_every_item

def __read_one_csv_file(inCsvFileName):
    try:
        callFailData=pd.read_csv(inCsvFileName,dtype={'失败原因': object})
        print(callFailData.columns)
        #print(callFailData.shape)
        return callFailData
    except:
        return None
    
def __clean_data_standard(callFailData):
    print('\n-----开始数据清理-----')
    shape_before=callFailData.shape[0]
    
    print('为所有的空值填充NULL...')
    callFailData=callFailData.fillna('null')
    
    callFailData['省直辖市']=callFailData['省/直辖市']
    callFailData['县区']=callFailData['县/区']
    
    callFailData['mFailCause']=callFailData['呼入呼出'].apply(__get_fail_cause)
    
    callFailData=callFailData.drop(['emmcid','地区码','发生时间','上报时间','异常进程名',
                                    '进程版本名','进程版本号','异常进程包名','软件系统类型',
                                    '国家','省/直辖市','县/区','详细地址','异常类型','呼入呼出',
                                    '结束位置码','结束基站编号','结束电话网络','结束数据网络',
                                    'isim支持情况','MBN版本信息','VOLTE配置信息','是否volte',
                                    '呼叫对方号码','保留字段一','保留字段二','异常次数','日志路径',
                                    'log信息'],axis=1)
    shape_after=callFailData.shape[0]
    print('-----数据清洗完成....'+str(shape_after)+'/'+str(shape_before)+'-----\n')
    return callFailData

def __get_fail_cause(name):
    return name.split('=')[-1]

def pdp_active_fail_main(path_raw_data,path_result):
    for file in os.listdir(path_raw_data):
        data_every_file=__read_one_csv_file(os.path.join(path_raw_data,file))
        
        data_every_file=__clean_data_standard(data_every_file)
        
        file_pre=os.path.splitext(file)[0]
        file_name_output_overall=file_pre+'_数据分析结果_整体.xls'
        file_name_output_every_item=file_pre+'_数据分析结果_Top分析.xls'
        
        write_data_into_excel_overall(data_every_file,os.path.join(path_result,file_name_output_overall))
        write_data_into_excel_every_item(data_every_file,os.path.join(path_result,file_name_output_every_item))
        
        
        