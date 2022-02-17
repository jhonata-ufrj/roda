#%%
from pandas._libs.tslibs import Period
from classe_planograma import Ponto, importar_vendas
import vmconnection as vmpay
import pandas as pd
import numpy as np
from IPython.display import display
import warnings
import json
import datetime

warnings.filterwarnings('ignore')

def puxar_picklists():
    DATASET_BASEDIR = "./datasets/"
    df_vendas = importar_vendas()

    df_vendas.sort_values(by=["data"],ascending=False)
    #try:
    df_picklist = pd.read_csv(r"datasets/picklist.csv",";",index_col=0)
    df_picklist_madruga = pd.read_csv("datasets/picklistMadruga.csv",";",index_col=0)

    df_picklist['data']=pd.to_datetime(df_picklist['data'],utc=True).dt.tz_convert('America/Sao_Paulo')
    df_picklist_madruga['data']=pd.to_datetime(df_picklist_madruga['data'],utc=True).dt.tz_convert('America/Sao_Paulo')

    b = df_picklist['data'].max()
    b = b - datetime.timedelta(days=4)
    b = b.replace( hour=00,minute=00,second=00, microsecond=0)
    df_picklist['data']=pd.to_datetime(df_picklist['data'],utc=True)
    df_picklist['data'] = df_picklist['data'].dt.tz_convert(tz='America/Sao_Paulo')
    

    df_vendas['data']=pd.to_datetime(df_vendas['data'],utc=True)
    df_vendas['data'] = df_vendas['data'].dt.tz_convert(tz='America/Sao_Paulo')
    

    df_picklist_madruga['data']=pd.to_datetime(df_picklist_madruga['data'],utc=True)
    df_picklist_madruga['data'] = df_picklist_madruga['data'].dt.tz_convert(tz='America/Sao_Paulo')

    df_picklist = df_picklist.loc[df_picklist['data']<pd.to_datetime(b).tz_convert(tz='America/Sao_Paulo')]
    df_picklist_madruga = df_picklist_madruga.loc[df_picklist_madruga['data']<pd.to_datetime(b).tz_convert(tz='America/Sao_Paulo')]
    df_vendas = df_vendas.loc[df_vendas['data']>=pd.to_datetime(b).tz_convert(tz='America/Sao_Paulo')]

    print(b)
    data_inicio = b
    #except:
        #df_picklist=pd.DataFrame()
        #data_inicio= datetime.date(2016,10,1)
        #print("Arquivo não encontrado")



    pdvs = df_vendas[['location_id',"local","machine_id","installation_id"]].drop_duplicates(subset=['location_id',"machine_id","installation_id"],keep='first', ignore_index=True)  

    nome_pdvs=df_vendas[['location_id','local']].drop_duplicates(subset=['location_id'],keep='last', ignore_index=True)

    id_picks=[]
    for index,row in pdvs.iterrows():
        location_id = row['location_id']
        local = row['local']
        machine_id = row['machine_id']
        installation_id= row['installation_id']
        print(local)
        for p in vmpay.VMConnection.getPickListsId(machine_id,installation_id,data_inicio):
            pick_id = p['id']
            data_pick=p['created_at']
            id_picks.append({"pick_id":pick_id,"location_id":location_id,"machine_id":machine_id,"installation_id":installation_id,"data":data_pick})
    df_pick_id = pd.DataFrame.from_dict(id_picks)
    df_pick_id = df_pick_id.drop_duplicates(subset="pick_id")

    erros =[]
    dados_pick=[]
    for index,row in df_pick_id.iterrows():
        location_id = row['location_id']
        machine_id = row['machine_id']
        installation_id= row['installation_id']
        pick_id = row['pick_id']
        data=row['data']
        print(pick_id,data)
        try:
          pick=vmpay.VMConnection.getPickLists(machine_id,installation_id,pick_id)
          planogram_id = pick['planogram_id']
          pick_items = pick['items']
          for b in pick_items:
              quantidade = b['quantity']
              good_id = b['good_id']
              ignorado = b['ignored'] 
              if ignorado ==False:
                dados_pick.append({"pick_id":pick_id,"location_id":location_id,"machine_id":machine_id,"installation_id":installation_id,"data":data,'planogram_id':planogram_id,"good_id":good_id,"quantidade":quantidade})
        except:
          print("ERRO")
          erros.append({"pick_id":pick_id,"location_id":location_id,"machine_id":machine_id,"installation_id":installation_id,"data":data})

    df_pick = pd.DataFrame.from_dict(dados_pick)
    df= pd.merge(df_pick,nome_pdvs,how= "left",on="location_id")
    
    df_erros = pd.DataFrame.from_dict(erros)
    df_erros.to_csv('erros_pick.csv')

    lista_produto=[]
    for produto in vmpay.VMConnection.getProducts():
      nome=produto["name"]
      good_id= produto['id']

      lista_produto.append({"good_name":nome,"good_id":good_id})
    df_produto = pd.DataFrame.from_dict(lista_produto)
    df = pd.merge(df,df_produto,how="left",on="good_id")
    

    dataframe2=pd.concat([df_picklist,df],ignore_index=True)
    dataframe2['data']=pd.to_datetime(dataframe2['data'],utc=True).dt.tz_convert('America/Sao_Paulo')
    dataframe2.to_csv(DATASET_BASEDIR + '/picklist.csv', sep=";")
    print("Picklists Puxadas")

    picklist= df
    df2 = picklist[['good_name']]
    df2 = df2.drop_duplicates()

    lista =[]
    for index,row in df2.iterrows():
        nome_produto = row['good_name']
        try:
            if "Sorvete" in nome_produto or "Picolé" in nome_produto or "Corneto" in nome_produto:
                lista.append(nome_produto)
        except:
            pass


    picklist['data']= pd.to_datetime(picklist['data'])
    picklist['hora'] = picklist['data'].dt.hour
    picklist.loc[(picklist['hora']>=21)&(picklist['hora']<=23),'dia'] = picklist['data'].dt.date 
    picklist.loc[(picklist['hora']>=0)&(picklist['hora']<=9),'dia'] = picklist['data'].dt.date- datetime.timedelta(days=1)
    df = picklist.dropna()
    from datetime import date
    df = df.loc[df['dia']>=date(2020,11,8)]

    df = df[~df['good_name'].isin(lista)]

    dataframe3=pd.concat([df_picklist_madruga,df],ignore_index=True)
    dataframe3['data']=pd.to_datetime(dataframe3['data'],utc=True).dt.tz_convert('America/Sao_Paulo')
    

    dataframe3.to_csv('datasets/picklistMadruga.csv',';')
    print("Picklists Ajustadas")
    return df
    
def ruptura():
#   df_picklists=pd.read_csv(r'picklistMadruga.csv',";",index_col=0)
  df_picklists = puxar_picklists()
  try:
    df_ruptura = pd.read_csv(r'datasets/Ruptura.csv',";",index_col=0)
  except:
    df_ruptura = pd.DataFrame()
    print("Arquivo não encontrado")
     
  print("CALCULANDO RUPTURA")
  locais = df_picklists[['machine_id','installation_id','local','planogram_id']].drop_duplicates()
  
  df_base = pd.read_csv(r"datasets/picklist.csv",";",index_col=0)
  df_base['data']=pd.to_datetime(df_base['data'])

  df_base1 = df_base[['location_id']].drop_duplicates()


  nome_pdvs=df_base[['location_id','local']].drop_duplicates(subset=['location_id'],keep='last', ignore_index=True)  
  
  lista_inicio=[]
  for index, row in df_base1.iterrows():
    local = row['location_id']
    df_base_aux = df_base[df_base['location_id']==local]
    
    data_min = df_base_aux['data'].min()
    data_min= pd.to_datetime(data_min) +datetime.timedelta(days=4)
    df_base_aux = df_base_aux[df_base_aux['data']<data_min]
    df_base_aux = df_base_aux[['pick_id']].drop_duplicates()
    for index, row in df_base_aux.iterrows():
      id_pick = row['pick_id']
      lista_inicio.append(id_pick)

  df_picklists = df_picklists[~df_picklists['pick_id'].isin(lista_inicio)]

  

  planogramas_lista = []
  for index, row in locais.iterrows():
      print(row["local"])
      machine_id = row["machine_id"]
      installation_id = row['installation_id']
      planogram_id = row['planogram_id']
      plan = pd.json_normalize(vmpay.VMConnection.getSpecificPlanogram(machine_id,installation_id,planogram_id))
      planogramas_lista.append(plan)

  plano = pd.concat(planogramas_lista).reset_index()

  planogramas = []
  for x in plano.index:
      teste = pd.json_normalize(plano.loc[x,'items'])
      planogramas.append(teste)


  plan = pd.concat(planogramas)[['planogram_id','good_id','par_level']]
  plan['planogram_id'] = plan['planogram_id'].astype(int)
  plan['good_id'] = plan['good_id'].astype(int)
  plan.fillna(0, inplace=True)


  reabastecimento = df_picklists[~df_picklists.good_name.str.contains('Sorvete|Picolé')][['location_id','planogram_id','good_id','good_name','dia','quantidade']]
  reabastecimento['good_id'] = reabastecimento['good_id'].astype(int)
  reabastecimento['planogram_id'] = reabastecimento['planogram_id'].astype(int)

  quantidades = reabastecimento.merge(plan, on = ['planogram_id','good_id'], how = 'inner')
  quantidades.fillna(0,inplace=True)

  quantidades['par_level'] = quantidades['par_level'].astype(int)
  quantidades['quantidade'] = quantidades['quantidade'].astype(int)

  quantidades['qtd_local'] = quantidades['par_level'] - quantidades['quantidade']

  quantidades = quantidades[quantidades['quantidade']>0]
  quantidades = quantidades.drop_duplicates()

  # quantidades['Mês'] = pd.to_datetime(quantidades['dia']).dt.to_period('M')

  prod_pick = quantidades.groupby(['location_id',"dia"])['good_id'].count().reset_index()

  aux_rupt= quantidades[quantidades['qtd_local']==0]
  prod_rup = aux_rupt.groupby(['location_id',"dia"])['good_id'].count().reset_index()


  rupt = prod_pick.merge(prod_rup,on=['location_id',"dia"],how='inner')
  
  rupt=rupt.rename(columns={"good_id_x":"Total","good_id_y":"Rupturas"})
  rupt=rupt.fillna(0)
  rupt['Taxa Ruptura'] = rupt["Rupturas"]/rupt["Total"]

  rupt= pd.merge(rupt,nome_pdvs,how= "left",on="location_id")
  
  df_rupt = pd.concat([df_ruptura,rupt],ignore_index=True)

  df_rupt = df_rupt.drop_duplicates()

  
  df_rupt.to_csv("datasets/Ruptura.csv",sep=";")

  return df_rupt
#%%

a = ruptura()
n = 28
locais = a['location_id',"local"].drop_duplicates
for index, row in locais.iterrows():
  location_id=row['location_id'] 
  local = row['local']
  df_aux= a[a['location_id']==location_id]
  data_inicio = df_aux['dia'].min()
  data_final = df_aux['dia'].max()
  while data_inicio<data_final:
    
#%%

# %%
