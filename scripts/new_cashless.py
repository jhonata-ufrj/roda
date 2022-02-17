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



# Separar vendas para planilha de preços
df = pandas.read_csv("./datasets/detached.csv",";",index_col=0,low_memory=False)

# # # df1= df1[['Requisição','Local','Valor (R$)','Número do cartão']]
# df['occurred_at'] =pandas.to_datetime(df['occurred_at'],utc=False) 
# df['occurred_at'] = df['occurred_at'].dt.tz_localize(None)
# df["status"]=df[df['status']=="OK"]
# df= df[df.location_name.str.contains('AMERICAN|Barra Corinto|BARRA GOLDEN|Connect|FOCUS|GOLDEN AGE|Hydra|Parque das Ár|SAN FIL|VALE DE ITA|VIVENDAS DA|VILLAGE GARDEN')]
# df = df.loc[df['occurred_at']>=datetime(2021, 9, 1)]

#import pdb;pdb.set_trace()
# df1 = df[df['location_name'].str.contains("FGV")]
# df1 = df1[['location_name','machine_id','installation_id']]
# df1=df1.drop_duplicates()
# df1 = df1.groupby(['Requisição','Local','Número do cartão']).sum(['Valor (R$)'])

# df.to_csv('Vendas.csv')
# import pdb; pdb.set_trace()

# df['occurred_at'] =pandas.to_datetime(df['occurred_at'],utc=False) 
# df['occurred_at'] = df['occurred_at'].dt.tz_localize(None)
# df = df.loc[df['occurred_at']>=datetime(2021, 6, 1)]
# df = df[df['good_name'].str.contains("Seara",na=False)]
# df = df[['good_name',"good_upc_code",'transaction_value']]
# df.to_csv("VendasSeara.csv",sep=";")
# import pdb;pdb.set_trace()

class VMConnection(object):
  # URL base da vmpay
  BASEURL = 'http://vmpay.vertitecnologia.com.br/api/v1/%s'
  # Formatos que vão ser enviados apra a api
  HEADERS = { 'content-type': 'application/json; charset=utf-8' }
  # Token de acesso à plataforma
  # PAYLOAD = { "access_token": "X6WktVwPeKagMHZh2iBBoEdK2cZK3h3TWQN5ZDDT" }
  PAYLOAD = { "access_token": "X6WktVwPeKagMHZh2iBBoEdK2cZK3h3TWQN5ZDDT" }
  
  # Diretórios
  DATASET_BASEDIR = "./datasets/"#Vendas/" #% datetime.now().strftime('%d%m%Y%H%M%S')

  def __init__(self, workspace='./datasets'):
    '''
      Cria o workspace para tratar as planilhas.
      FIX: _mission_cache_control: Usei essa pasta pra salvar csvs de controle porque estourou a memória de dados
      quando chegou a um limite de páginas.
    '''
    self.workspace = workspace
    # cria os diretórios caso não existam
    Path(self.DATASET_BASEDIR).mkdir(parents=True, exist_ok=True)

  def getPurchases(self, save=True, **kwargs) -> list:
    '''
      Busca as vendas de acordo com os parâmetros da função.
      As vendas são lançadas na VMPay produto a produto.

      Parametros:
      ===========
      - save (boolean):
        quando for true, salva a planilha com os dados de todas as vendas

      FIX: Por algum motivo ele não consegue achar uma data cujo registro não existe. Acabei mantendo a estrutura para 
      buscar absolutamente todas as vendas cadastradas na vmpay.
    '''
    print("OK")
    df = pandas.read_csv("./datasets/detached.csv",";",index_col=0)
    
    print(df.head())
    df['occurred_at'] =pandas.to_datetime(df['occurred_at']) 
    f = datetime.now()
    dia = str( '%02d' % f.day)
    mes = str('%02d' %f.month)
    ano = str(f.year)
    f = f.replace( hour=23,minute=59,second=59, microsecond=0)
    dataini1 = dia+"/"+mes+"/"+ano+" 10:00:00"
    # uldata1 = datetime.strptime(uldata, '%Y-%m-%dT%H:%M:%S.%f%z')
    # uldata1 = uldata1 + timedelta(days=1)
    # uldata1 = uldata1.replace( hour=10,minute=00,second=1, microsecond=0)
    
    b = max(df['occurred_at'])
    # b = datetime(2021,8,7)
    # dia2 = str( '%02d' % b.day)
    # mes2 = str('%02d' %b.month)
    # ano2 = str(b.year)
    # uldata1 = dia2+"/"+mes2+"/"+ano2+" 10:00:00"
    uldata1 = b
    # import pdb; pdb.set_trace()
    # Adiciona dois parâmetros na requisição: page (página), per_page (quantidade por página)
    payload = self.PAYLOAD
    payload['page'] = 1
    payload['per_page'] = 1000
    payload['start_date'] = f - timedelta(days = 1)
    payload['end_date'] = f
    payload['status'] = "ok"
    # Adiciona os parâmetros da documentação (https://vmpay-api.readthedocs.io/en/latest/reports/vend.html)
    for key, value in kwargs.items():
      payload[key] = value
    # variáveis de apoio:
    dataflow = list()
    # Faz a primeira requisição para inicializar a variável de resposta.
    # NOTE: Isso pode ser otimizado na versão do Python 3.8. Veja a PEP 572.
    contador = 1
    response = requests.get(self.BASEURL % 'cashless_facts', params=payload, headers=self.HEADERS)
    while response.ok and response.json() != []:
      print('[DEBUG | %s] - Página %d, Último dia capturado: %s' % (
          datetime.now(), payload['page'], response.json()[-1]['occurred_at']
        )
      )
      # dei um flatten no dicionário para não ter entradas aninhadas
      for data in response.json():
        dataflow.append(flatten(data, reducer=self.underscore_reducer))
      # faz a nova requisição com a nova página
      payload['page'] = payload['page'] + 1
      response = requests.get(self.BASEURL % 'cashless_facts', params=payload, headers=self.HEADERS)
      utc=pytz.UTC
      start_time = uldata1.replace(tzinfo=utc)
      end_time = payload['start_date'].replace(tzinfo=utc)
      if response.json()==[] and end_time>=start_time:
        payload['page'] = 1
        payload['start_date'] = f - timedelta(days = contador+1)
        payload['end_date'] = f - timedelta(days = contador)
        contador += 1
        response = requests.get(self.BASEURL % 'cashless_facts', params=payload, headers=self.HEADERS)
        # import pdb;pdb.set_trace()
    
    # Salva os dados não agrupados
    if save:

      dataframe = df
      dataframe1= pandas.DataFrame(dataflow)
      dataframe2=pandas.concat([dataframe,dataframe1],ignore_index=True)
      # dataframe2['occurred_at']= pandas.to_datetime(dataframe2['occurred_at'])
      # end_date = datetime.now(tz=timezone.utc) + timedelta(days=-1)
      # end_date = end_date.replace( hour=23,minute=59,second=59, microsecond=0)
 
      # mask = dataframe2['occurred_at'] <= end_date
      
      # dataframe3 = dataframe2.loc[mask]
      dataframe2.drop_duplicates()
      dataframe2['occurred_at']=pandas.to_datetime(dataframe2['occurred_at'],utc=True).dt.tz_convert('America/Sao_Paulo')
      dataframe2.to_csv(self.DATASET_BASEDIR + '/detached.csv', sep=";")
      # dataframe3.to_csv(self.DATASET_BASEDIR + '/detached.csv', sep=";")



    return dataframe2

  def underscore_reducer(self, k1, k2):
    if k1 is None:
      return k2
    else:
      return k1 + "_" + k2

vmpay = VMConnection()
purchases = vmpay.getPurchases()
