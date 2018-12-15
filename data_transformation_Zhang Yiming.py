# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 01:21:57 2018

@author: Zhang Yiming
"""
import pandas as pd
import re
import numpy as np
deal_level_data = pd.read_csv('D:\AFPD\Projects\Data transformation\data\deal_level_data.csv')
data_set = list(deal_level_data)
number_of_row = len(deal_level_data)
quarter_number = [i for i in range(-12,13)]

#Find quarter nubmer#

data_set_name = [''] * len(data_set)
for i in range(14,len(data_set)):
    data_set_name[i] = re.sub('\d','',data_set[i],flags=0)
    data_set_name[i] = re.sub('_','',data_set_name[i],flags=0)
df_data_set_name = pd.DataFrame(data_set_name)
data_set = list(deal_level_data)

data_set_quart = [''] * len(data_set)
for i in range(14,len(data_set)):
    if '__' in data_set[i]:
        data_set_quart[i] = - int(re.sub('\D','',data_set[i],flags=0))
    elif re.sub('\D','',data_set[i],flags=0) == '':
        data_set_quart[i] = 0
    else:
        data_set_quart[i] = int(re.sub('\D','',data_set[i],flags=0))   
df_data_set_quart = pd.DataFrame(data_set_quart)
data_set = list(deal_level_data)
#Find quarter nubmer#

#This part decide the column number#
name_order = pd.Series(data_set_name).drop_duplicates()
name_order[0]=np.nan
name_order.dropna(inplace = True)
columns_0 = data_set[0:14]+['quartar_to_data']+list(name_order)
df_total = pd.DataFrame(data_set[0:14]+['quartar_to_data']+list(name_order)).T
df_total.columns = columns_0

#This part is the a loop that go through all the rows and find the information to form a new dataframe#
for counter in range(number of row):
    #this part is to get detail numbers#
    df1 = deal_level_data.loc[counter]
    list_combined = [list(df1),data_set_quart,data_set_name]
    df_combined = pd.DataFrame([list(df1),data_set_quart,data_set_name], index = ['deal','quarter','name']).T
    df_final = pd.pivot_table(df_combined, values = 'deal', index=['quarter'],columns = 'name',aggfunc=np.sum)
    df_final = df_final[list(name_order)]
    df_final = df_final.reset_index(drop=True)
    df_final.replace(0,np.nan, inplace=True)
    #this part is to get detail numbers#
    
    #This part is to get heads#
    df1_multi_head = pd.DataFrame(df1.tolist()[0:14]).T
    df1_concat = pd.concat([df1_multi_head] * 25).reset_index(drop=True)
    df1_concat['14'] = quarter_number
    df1_concat = pd.concat([df1_concat,df_final], axis=1)
    df1_concat.columns = columns_0
    df_total = pd.concat([df_total,df1_concat]) #concatenate different dataframe into one#
df_total.to_csv('D:\AFPD\Projects\Data transformation\data\quarter_level_data_new_Zhang Yiming.csv',index = False,header = False)
