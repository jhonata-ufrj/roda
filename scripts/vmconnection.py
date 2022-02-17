'''
Importar vendas de API.
'''

import os
import json
from pathlib import Path
from datetime import datetime, timedelta
import csv
import pytz
import numpy
import pandas
import requests
import matplotlib.pyplot as pyplot
from flatten_dict import flatten
from datetime import timezone


class VMConnection(object):
  # URL base da vmpay
  BASEURL = 'http://vmpay.vertitecnologia.com.br/api/v1/%s'
  # Formatos que vão ser enviados apra a api
  HEADERS = { 'content-type': 'application/json; charset=utf-8' }
  # Token de acesso à plataforma
  PAYLOAD = { "access_token": "X6WktVwPeKagMHZh2iBBoEdK2cZK3h3TWQN5ZDDT" }
  # Diretórios
  DATASET_BASEDIR = "../datasets/vendas/"#Vendas/" #% datetime.now().strftime('%d%m%Y%H%M%S')

  def __init__(self, workspace='../datasets/vendas'):
    '''
      Cria o workspace para tratar as planilhas.
      FIX: _mission_cache_control: Usei essa pasta pra salvar csvs de controle porque estourou a memória de dados
      quando chegou a um limite de páginas.
    '''
    self.workspace = workspace
    # cria os diretórios caso não existam
    Path(self.DATASET_BASEDIR).mkdir(parents=True, exist_ok=True)

  def getPurchases(self, venda_antiga, save=True, **kwargs) -> list:
    '''
      Busca as vendas de acordo com os parâmetros da função.
      As vendas são lançadas na VMPay produto a produto.

      Parametros:
      ===========
      - venda_antiga: arquivo com vendas antigas
      - save (boolean):
        quando for true, salva a planilha com os dados de todas as vendas

      FIX: Por algum motivo ele não consegue achar uma data cujo registro não existe. Acabei mantendo a estrutura para 
      buscar absolutamente todas as vendas cadastradas na vmpay.
    '''
    
    f = datetime.now()
    dia = str('%02d' % f.day)
    mes = str('%02d' %f.month)
    ano = str(f.year)
    dataini1 = dia+"/"+mes+"/"+ano+" 10:00:00"
    uldata = max(venda_antiga['occurred_at'])
    uldata1 = datetime.strptime(uldata, '%Y-%m-%dT%H:%M:%S.%f%z')
    # uldata1 = uldata1 + timedelta(days=1)
    uldata1 = uldata1.replace( hour=10,minute=00,second=1, microsecond=0)
    # import pdb; pdb.set_trace()
    # Adiciona dois parâmetros na requisição: page (página), per_page (quantidade por página)
    payload = self.PAYLOAD
    payload['page'] = 1
    payload['per_page'] = 1000
    payload['start_date'] = uldata1
    payload['end_date'] = dataini1
    
    # Adiciona os parâmetros da documentação (https://vmpay-api.readthedocs.io/en/latest/reports/vend.html)
    for key, value in kwargs.items():
      payload[key] = value
    # variáveis de apoio:
    dataflow = list()
    # Faz a primeira requisição para inicializar a variável de resposta.
    # NOTE: Isso pode ser otimizado na versão do Python 3.8. Veja a PEP 572.
    response = requests.get(self.BASEURL % 'cashless_transactions', params=payload, headers=self.HEADERS)
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
      response = requests.get(self.BASEURL % 'cashless_transactions', params=payload, headers=self.HEADERS)

    
    # Salva os dados não agrupados

      dataframe  = pandas.DataFrame(venda_antiga)
      dataframe1 = pandas.DataFrame(dataflow)
      dataframe2 = pandas.concat([dataframe,dataframe1],ignore_index=True)
      # dataframe2['occurred_at']= pandas.to_datetime(dataframe2['occurred_at'])
      # end_date = datetime.now(tz=timezone.utc) + timedelta(days=-1)
      # end_date = end_date.replace( hour=23,minute=59,second=59, microsecond=0)
 
      # mask = dataframe2['occurred_at'] <= end_date
    if save:    
      # dataframe3 = dataframe2.loc[mask]
      if "dataframe2" in locals():
        dataframe2.to_csv(self.DATASET_BASEDIR + 'detached.csv', sep=";", index = False)
      # dataframe3.to_csv(self.DATASET_BASEDIR + '/detached.csv', sep=";")

    if "dataframe2" in locals():
      return dataframe2
    else:
      print('Não conseguiu acessar a API. Retornando  dataframe antigo')
      return venda_antiga
      
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
      print("-------OK---------")
      df = pandas.read_csv(r"../datasets/vendas/detached.csv",";",index_col=0,low_memory=False)
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
      dataframe2.to_csv(self.DATASET_BASEDIR + 'detached.csv', sep=";")
      print(datetime.now())

    return dataframe2

  def underscore_reducer(self, k1, k2):
    if k1 is None:
      return k2
    else:
      return k1 + "_" + k2
  
  @classmethod
  def getMachines(self):
    # Cria o pedido à vmpay (url de acesso, parâmetros de autenticação, cabeçalho da requisição)
    response = requests.get(self.BASEURL % 'machines', params=self.PAYLOAD, headers=self.HEADERS)
    
    return response.json()

  #verifica os produtos cadastrados, é preciso pegar o ID dos produtos OBS:NÃO É O QUE APARECE NA INTERFACE DA VMPAY
  @classmethod
  def getProducts(self):
    # Cria o pedido à vmpay (url de acesso, parâmetros de autenticação, cabeçalho da requisição)
    response = requests.get(self.BASEURL % 'products', params=self.PAYLOAD, headers=self.HEADERS)
    return response.json()

  @classmethod
  def getPlanogram(self,machine,inst):
    machine = str(machine)
    inst = str(inst)
    URL = 'machines/'+ str(machine)+'/installations/'+ str(inst) +'/planograms'
    response = requests.get(self.BASEURL % URL , params=self.PAYLOAD, headers=self.HEADERS)
    return response.json()

  @classmethod
  def getSpecificPlanogram(self,machine,inst,planId):
    URL = 'machines/'+ str(machine)+'/installations/'+ str(inst) +'/planograms/' + str(planId)
    response = requests.get(self.BASEURL % URL , params=self.PAYLOAD, headers=self.HEADERS)
    return response.json()


  #cria o planograma para a máquina desejada, o planograma fica pendente, então é possível verificá-lo antes de deixá-lo ativo
  @classmethod
  def postPlanograms(self,machineID,instaID,planogramaFuturo):
    #inserir machine_id e instalation_id do local que deseja alterar o planograma
    machine_id= machineID
    installation_id = instaID
    plan_futuro = planogramaFuturo
    print(plan_futuro)
    
    x = { 
          "planogram": {
                        "details": "Planograma criado via script",      
                        "items_attributes": plan_futuro
                        }
         }
    response = requests.post(self.BASEURL % 'machines/'+ machine_id +'/installations/'+ installation_id +'/planograms', params=self.PAYLOAD,data=json.dumps(x), headers=self.HEADERS)
    print(response.content)

  #atualiza o planograma para a máquina desejada, o planograma fica pendente, então é possível verificá-lo antes de deixá-lo ativo
  @classmethod
  def patchPlanograms(self,machineID,instaID,planogramaID,planogramaFuturo):
    #inserir machine_id e instalation_id do local que deseja alterar o planograma
    machine_id= machineID
    installation_id = instaID
    x = { 
          "planogram": {
                        "details": "Planograma criado via script",      
                        "items_attributes": planogramaFuturo
                        }
         }
    response = requests.patch(self.BASEURL % 'machines/'+ machine_id +'/installations/'+ installation_id +'/planograms/' + planogramaID, params=self.PAYLOAD,data=json.dumps(x), headers=self.HEADERS)
    print(response.content)
  
  #deleta planograma pendente
  @classmethod
  def delPlanograms(self,machineID,instaID,planogramaID):
    #inserir machine_id e instalation_id do local que deseja alterar o planograma
    machine_id= machineID
    installation_id = instaID
    response = requests.delete(self.BASEURL % 'machines/'+ machine_id +'/installations/'+ installation_id +'/planograms/' + planogramaID, params=self.PAYLOAD, headers=self.HEADERS)
    print(response.content)

  @classmethod  
  def getPickListsId(self,machine,inst,data_inicio):
      URL = 'machines/'+ str(machine)+'/installations/'+ str(inst) +'/pick_lists'
      payload = self.PAYLOAD
      payload['page'] = 1
      payload['per_page'] = 100
      payload['updated_since']= data_inicio
      response = requests.get(self.BASEURL % URL, params=payload, headers=self.HEADERS)
      picksIds = []
      while response.ok and response.json() != []:
        for data in response.json():
          picksIds.append(data)
        payload['page'] = payload['page'] + 1
        response = requests.get(self.BASEURL % URL, params=payload, headers=self.HEADERS)
      #return response.json()
      return picksIds

  @classmethod
  def getPickLists(self,machine,inst,id):
    #GET /api/v1/machines/[machine_id]/installations/[installation_id]/pick_lists/[id]
    URL = 'machines/'+ str(machine)+'/installations/'+ str(inst) +'/pick_lists/' + str(id)+'?show_all_items=true'
    response = requests.get(self.BASEURL % URL, params=self.PAYLOAD, headers=self.HEADERS)
    return response.json()
  
  @classmethod
  def getInventoryAdjustments(self,machine,inst):
      URL = 'machines/'+ str(machine)+'/installations/'+ str(inst) + '/inventory_adjustments'
      response = requests.get(self.BASEURL % URL, params = self.PAYLOAD, headers = self.HEADERS)
      return response.json()
  
  @classmethod
  def getLocations(self):
      response = requests.get(self.BASEURL % 'locations', params = self.PAYLOAD, headers = self.HEADERS)
      return response.json()
