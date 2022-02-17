#%%
import os
import json
from pathlib import Path
from datetime import datetime, time, timedelta
import csv

import numpy
import pandas
import requests
import matplotlib.pyplot as pyplot
from flatten_dict import flatten
from datetime import timezone
from datetime import date
import pytz
from custo_amlabs import Tricks


# df_custos1= pandas.read_csv(r"Custos.csv",",",index_col=0,low_memory=False)
df_custos1 = Tricks().gerar_custos()
df_custos1['data de recebimento']=pandas.to_datetime(df_custos1['data de recebimento'])


df_custos1 =df_custos1[['código do produto','produto','data de recebimento','Custo Final']]

produtos = df_custos1['código do produto'].drop_duplicates().reset_index()

nova_lista_custo=[]
for index, row in produtos.iterrows():
    codigo_produto = row['código do produto']
    
    aux_df_custos = df_custos1[df_custos1['código do produto']==codigo_produto]
    aux_datas = aux_df_custos['data de recebimento']
    for index,row in aux_datas.iteritems():
        data_inicio = row
        aux_df_custos2 = aux_df_custos[aux_df_custos['data de recebimento']> pandas.to_datetime(data_inicio)]
        data_final = aux_df_custos2['data de recebimento'].min()
        data_final = pandas.to_datetime(data_final)
        custo = aux_df_custos[aux_df_custos['data de recebimento']==data_inicio]['Custo Final'].iloc[0]
        produto =aux_df_custos[aux_df_custos['data de recebimento']==data_inicio]['produto'].iloc[0]

        if pandas.isnull(data_final):
            data_final= datetime.today()

        nova_lista_custo.append({'upc_code':codigo_produto,"good_name":produto,"Data de Recebimento":data_inicio,"Data Limite":data_final,"Custo Final":custo})




df_custo_final = pandas.DataFrame.from_dict(nova_lista_custo)
df_custo_final['Data de Recebimento'] = df_custo_final['Data de Recebimento'].dt.normalize()
df_custo_final['Data Limite'] = df_custo_final['Data Limite'].dt.normalize()
import pdb;pdb.set_trace()
df_custo_final = df_custo_final.drop_duplicates(subset=['upc_code','Data de Recebimento'])
df_custo_final.to_csv("Base Custo.csv")

    

# %%
