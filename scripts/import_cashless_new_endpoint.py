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

  def getPurchasesNewEndPoint(self, save=True, **kwargs) -> list:
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
    try:
      df = pandas.read_csv(r"datasets\detached.csv",";",index_col=0,low_memory=False)
      df['occurred_at']=pandas.to_datetime(df['occurred_at'],utc=True).dt.tz_convert('America/Sao_Paulo')
      b = df['occurred_at'].max()
      b = b.replace( hour=00,minute=00,second=00, microsecond=0)
      df['occurred_at']=pandas.to_datetime(df['occurred_at'],utc=True)
      df['occurred_at'] = df['occurred_at'].dt.tz_convert(tz='America/Sao_Paulo')
      df = df.loc[df['occurred_at']<pandas.to_datetime(b).tz_convert(tz='America/Sao_Paulo')]
      print(b)
    except:
      b = datetime(2016,10,1)
      df=pandas.DataFrame()

    f = datetime.now()
    f = f.replace( hour=23,minute=59,second=59, microsecond=0)
    uldata1 = b
    # Adiciona dois parâmetros na requisição: page (página), per_page (quantidade por página)
    payload = self.PAYLOAD
    payload['page'] = 1
    payload['per_page'] = 1000
    payload['start_date'] = f- timedelta(days = 1)
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
    utc=pytz.UTC
    try:
      start_time = uldata1.replace(tzinfo=utc)
    except:
        start_time = uldata1
    try:
        end_time = payload['end_date'].replace(tzinfo=utc)
    except:
        end_time = payload['end_date']  

    while response.ok and end_time>start_time:
      try:
        print('[DEBUG | %s] - Página %d, Último dia capturado: %s' % (
            datetime.now(), payload['page'], response.json()[-1]['occurred_at']
          )
        )
      except:
        print("Dia sem vendas")
            
      for data in response.json():
        dataflow.append(flatten(data, reducer=self.underscore_reducer))
      # faz a nova requisição com a nova página
      payload['page'] = payload['page'] + 1
      response = requests.get(self.BASEURL % 'cashless_facts', params=payload, headers=self.HEADERS)
      
      try:
        start_time = uldata1.replace(tzinfo=utc)
      except:
        start_time = uldata1
      try:
        end_time = payload['end_date'].replace(tzinfo=utc)

      except:
        end_time = payload['end_date']  
      

      if response.json()==[] and end_time>start_time:
        payload['page'] = 1
        payload['start_date'] = f - timedelta(days = contador+1)
        payload['end_date'] = f - timedelta(days = contador)
              
        contador += 1
        try:
          end_time = payload['end_date'].replace(tzinfo=utc)
        except:
          end_time = payload['end_date']  
        if end_time>start_time:
          response = requests.get(self.BASEURL % 'cashless_facts', params=payload, headers=self.HEADERS)

    # Salva os dados não agrupados
    if save:
      print(datetime.now())
      dataframe = df
      dataframe1= pandas.DataFrame.from_dict(dataflow)
      dataframe2=pandas.concat([dataframe,dataframe1],ignore_index=True)
      dataframe2=dataframe2.drop_duplicates()
      dataframe2['occurred_at']=pandas.to_datetime(dataframe2['occurred_at'],utc=True).dt.tz_convert('America/Sao_Paulo')
      dataframe2.to_csv(self.DATASET_BASEDIR + '/detached.csv', sep=";")
      print(datetime.now())

    return dataframe2

  def underscore_reducer(self, k1, k2):
    if k1 is None:
      return k2
    else:
      return k1 + "_" + k2
   
vmpay = VMConnection()
purchases = vmpay.getPurchasesNewEndPoint()
