#!/usr/bin/python3
# -*- coding: utf-8 -*- 

import os
from bigdata_in_callfail import big_data_in_call_fail_main, bigDataIn_plot_trend
from cloud_in_callfail import cloud_in_callfail_main, cloud_in_call_fail_plot_trend
from cloud_in_oos import cloud_in_oos_main, cloud_in_oos_plot_trend
from bigdata_out_callfail import big_data_out_call_fail_main,big_data_out_plot_trend
from cloud_out_callfail import cloud_out_callfail_main,cloud_out_call_fail_plot_trend
from cloud_out_oos import cloud_out_oos_main, cloud_out_oos_plot_trend
from cloud_in_pdp_fail import cloud_in_pdpfail_main, cloud_in_pdpfail_plot_trend
from cloud_out_pdp_fail import cloud_out_pdpfail_main,cloud_out_pdpfail_plot_trend

def main():

    print('''
----------------------------------------------
\tbigdata in call_fail 数据统计 按每周----11
\tbigdata in call_fail 数据统计 按每月----12
----------------------------------------------
\tbigdata out call_fail 数据统计 按每周----21
----------------------------------------------
\tcloud in call_fail 数据统计 按每周----31
\tcloud in call_fail 数据统计 按每月----32
----------------------------------------------
\tcloud out call_fail 数据统计 按每周----41
\tcloud out call_fail 数据统计 按每月----42
----------------------------------------------
\tcloud in oos 数据统计 按每周----51
\tcloud in oos 数据统计 按每月----52
----------------------------------------------
\tcloud out oos 数据统计 按每周----61
\tcloud out oos 数据统计 按每月----62
----------------------------------------------
\tcloud in PDP激活失败 数据统计 按每周----71
\tcloud in PDP激活失败 数据统计 按每月----72
----------------------------------------------
\tcloud out PDP激活失败 数据统计 按每周----81
\tcloud out PDP激活失败 数据统计 按每月----82
----------------------------------------------
    ''')

    selected = input('请选择要执行的项目:')
    print ('你选择的是第'+str(selected)+'项')

    #---bigdata in call_fail 数据统计 按每周
    if(selected=='11' or selected==11):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','bigdata_in_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','bigdata_in_reportdata','weeks')

        big_data_in_call_fail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_in_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_in_reportdata', '大数据内销掉话周趋势.xlsx')

        bigDataIn_plot_trend(path_raw_data, path_result)

    #---bigdata in call_fail 数据统计 按每月
    elif(selected=='12' or selected==12):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','bigdata_in_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','bigdata_in_reportdata','months')

        big_data_in_call_fail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_in_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_in_reportdata', '大数据内销掉话月趋势.xlsx')

        bigDataIn_plot_trend(path_raw_data, path_result)

    #--------------------------------------------
    #---bigdata out call_fail 数据统计 按每周
    if(selected=='21' or selected==21):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','bigdata_out_rawdata')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','bigdata_out_reportdata','weeks')

        big_data_out_call_fail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_out_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'bigdata_out_reportdata', '大数据外销掉话周趋势.xlsx')

        big_data_out_plot_trend(path_raw_data, path_result)

    #----------------------------------------------------------------
    #---cloud in call_fail 数据统计 按每周
    elif(selected=='31' or selected==31):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_callfail_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_callfail_reportdata','weeks')

        cloud_in_callfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_callfail_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_callfail_reportdata', '云诊断内销掉话周趋势.xlsx')

        cloud_in_call_fail_plot_trend(path_raw_data, path_result)

    #---cloud in call_fail 数据统计 按每月
    elif(selected=='32' or selected==32):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_callfail_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_callfail_reportdata','months')

        cloud_in_callfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_callfail_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_callfail_reportdata', '云诊断内销掉话月趋势.xlsx')

        cloud_in_call_fail_plot_trend(path_raw_data, path_result)

    #----------------------------------------------------------------
    #---cloud out call_fail 数据统计 按每周
    elif(selected=='41' or selected==41):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_callfailraw_data','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_callfail_reportdata','weeks')

        cloud_out_callfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_callfail_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_callfail_reportdata', '云诊断外销掉话周趋势.xlsx')

        cloud_out_call_fail_plot_trend(path_raw_data, path_result)

    #---cloud out call_fail 数据统计 按每月
    elif(selected=='42' or selected==42):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_callfail_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_callfail_reportdata','months')

        cloud_out_callfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_callfail_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_callfail_reportdata', '云诊断外销掉话月趋势.xlsx')

        cloud_out_call_fail_plot_trend(path_raw_data, path_result)

    #--------------------------------------------
    #---cloud in oos 数据统计 每周
    elif(selected=='51' or selected==51):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_oos_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_oos_reportdata','weeks')

        cloud_in_oos_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_oos_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_oos_reportdata', '云诊断内销掉网周趋势.xlsx')

        cloud_in_oos_plot_trend(path_raw_data, path_result)

    #---cloud in oos 数据统计 按每月
    elif(selected=='52' or selected==52):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_oos_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_oos_reportdata','months')

        cloud_in_oos_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_oos_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_oos_reportdata', '云诊断内销掉网月趋势.xlsx')

        cloud_in_oos_plot_trend(path_raw_data, path_result)

    #--------------------------------------------
    #---cloud out oos 数据统计 每周
    elif(selected=='61' or selected==61):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_oos_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_oos_reportdata','weeks')

        cloud_out_oos_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_oos_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_oos_reportdata', '云诊断外销掉网周趋势.xlsx')

        cloud_out_oos_plot_trend(path_raw_data, path_result)

    #---cloud out oos 数据统计 按每月
    elif(selected=='62' or selected==62):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_oos_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_oos_reportdata','months')

        cloud_out_oos_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_oos_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_oos_reportdata', '云诊断外销掉网周趋势.xlsx')

        cloud_out_oos_plot_trend(path_raw_data, path_result)

    #--------------------------------------------
    #---cloud in PDP激活失败 数据统计 每周
    elif(selected=='71' or selected==71):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_pdp_fail_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_pdp_fail_reportdata','weeks')

        cloud_in_pdpfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_pdp_fail_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_pdp_fail_reportdata', '云诊断内销PDP失败周趋势.xlsx')

        cloud_in_pdpfail_plot_trend(path_raw_data, path_result)

    #---cloud in PDP激活失败 数据统计 按每月
    elif(selected=='72' or selected==72):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_pdp_fail_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_in_pdp_fail_reportdata','months')

        cloud_in_pdpfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_pdp_fail_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_in_pdp_fail_reportdata', '云诊断内销PDP失败月趋势.xlsx')

        cloud_in_pdpfail_plot_trend(path_raw_data, path_result)

    #--------------------------------------------
    #---cloud out PDP激活失败 数据统计 每周
    elif(selected=='81' or selected==81):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_pdp_fail_rawdata','weeks')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_pdp_fail_reportdata','weeks')

        cloud_out_pdpfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_pdp_fail_reportdata', 'weeks')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_pdp_fail_reportdata', '云诊断外销PDP失败周趋势.xlsx')

        cloud_out_pdpfail_plot_trend(path_raw_data, path_result)

    #---cloud out PDP激活失败 数据统计 按每月
    elif(selected=='82' or selected==82):
        in_path=os.path.abspath('..')
        path_raw_data=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_pdp_fail_rawdata','months')
        path_result=os.path.join(in_path,'bigdata_cloud_datas','cloud_out_pdp_fail_reportdata','months')

        cloud_out_pdpfail_main(path_raw_data,path_result)

        in_path = os.path.abspath('..')
        path_raw_data = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_pdp_fail_reportdata', 'months')
        path_result = os.path.join(in_path,'bigdata_cloud_datas', 'cloud_out_pdp_fail_reportdata', '云诊断外销PDP失败月趋势.xlsx')

        cloud_out_pdpfail_plot_trend(path_raw_data, path_result)

    else:
        pass
        #print('your selected item is not working...')

if __name__=='__main__':
    main()
    
    
    
