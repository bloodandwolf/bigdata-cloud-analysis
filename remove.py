import pandas as pd
import os

pwd_path = os.path.abspath('.')
for file in os.listdir(pwd_path):
    if(file.endswith('.csv')):
        filename = os.path.join(pwd_path,file)
        print(filename)
        datas = pd.read_csv(filename)
        print(datas.columns)
        if('Unnamed: 0' in datas.columns):
            datas = datas.drop(['Unnamed: 0'],axis=1)
            datas.to_csv(filename,index=False)
