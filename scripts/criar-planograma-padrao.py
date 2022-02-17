#%%
import pandas as pd
from importar_datasets import importar_vendas, importar_limites_prateleira
from datetime import datetime, timedelta 
import simular_vendas as sim
from vmconnection import VMConnection

vendas = importar_vendas('detached', only_mc = True)
vendas = vendas[vendas['dia'] < datetime.today().date()]

#pdvs_usados = vendas[vendas['dia'] < datetime(2021,2,1).date()]['local'].drop_duplicates()
#vendas = vendas[vendas['local'].isin(list(pdvs_usados))]
vendas = vendas[vendas['dia'] > max(vendas['dia']) - timedelta(days = 30)]

vendas_por_dia = vendas.groupby(['dia','id_produto'])['valor'].count().reset_index() \
    .pivot_table(index = 'dia', columns = 'id_produto', values = 'valor').fillna(0)

locais_por_dia = vendas.groupby(['dia'])['local'].nunique()
#%%
locais_por_produto = (vendas.groupby(['id_produto'])['local'].nunique() / locais_por_dia.mean() > (4/48)).reset_index().rename(columns = {'local':'vendido'})

#%%
media_consumo = vendas_por_dia.divide(locais_por_dia,axis = 0).mean().reset_index().rename(columns = {0:'media'})
desvio_consumo = vendas_por_dia.divide(locais_por_dia,axis = 0).std().reset_index().rename(columns = {0:'desvio'})
dados = media_consumo.merge(desvio_consumo, on = 'id_produto', how = 'outer')
dados = dados.merge(locais_por_produto, on ='id_produto', how = 'outer')
dados['id_produto'] = dados['id_produto'].astype(str)
dados = dados[(dados['media'] > 0.01) & (dados['vendido'] == True)]
limites = importar_limites_prateleira('./datasets/limite_prateleira')
limites['id_produto'] = limites['id_produto'].astype(str)
dados = dados.merge(limites, on = 'id_produto', how = 'inner')
#%%
vm = VMConnection()

produtos = pd.DataFrame(vm.getProducts())
produtos = produtos[['id','name']].rename(columns = {'id':'id_produto','name':'produto'})
produtos['id_produto'] = produtos['id_produto'].astype(str)

dados = dados.merge(produtos, on = 'id_produto',how = 'left')

#### PUXAR PLANOGRAMA DE TODOS OS PONTOS PARA PEGAR CANALETAS

## Atualiza pontos de venda
locais = pd.DataFrame(vm.getLocations())
maquinas = pd.json_normalize(vm.getMachines())
locais = locais[['id','name']]
locais.columns = ['location_id','location_name']
maquinas = maquinas[['id','installation.location_id','installation.id']]
maquinas.columns = ['machine_id','location_id','installation_id']
locais = locais.merge(maquinas, on = ['location_id'], how='left')

locais = locais[~locais['machine_id'].isnull()]

pdvs = locais[['location_name','machine_id','installation_id']]
pdvs['machine_id'] = pdvs['machine_id'].astype(int)
pdvs['installation_id'] = pdvs['installation_id'].astype(int)

pdvs.to_csv('./datasets/pontos.csv', sep=';',decimal=',',encoding='latin-1',index=False)

dfs = []
for i in pdvs.index:
    try:
        local = pdvs.loc[i,'location_name']
        machine_id = pdvs.loc[i,'machine_id']
        inst_id = pdvs.loc[i,'installation_id']
        temp = pd.json_normalize(vm.getPlanogram(machine_id, inst_id)[-1]['items'])
        temp = temp[['good_id','name']]
        dfs.append(temp)
    except:
        pass

plan = pd.concat(dfs).drop_duplicates(subset = ['good_id','name'])
plan['good_id'] = plan['good_id'].astype(str)
dados = dados.merge(plan, left_on = ['id_produto'], right_on = ['good_id'], how = 'left')


dados['canaleta'] = dados['name'].str.split(pat = '-',expand = True)[1].str[-2:]

dados['canaleta'] = dados['canaleta'].str.replace('PP','PM')
# Produtos com PET no nome são as bebidas de garrafas grandes, que são otimizadas em separado do resto da geladeira. Por isso criamos outro nome.
dados.loc[dados['produto'].str.contains(' PET'),'canaleta'] = 'RG'

dados['fileiras_ocupadas'] = 0

dados = dados.drop_duplicates(subset = ['good_id'])

espacos = {'CV':35,'CC':16,'RG':8,'PM':None,'GB':None}
produtos_lista = []
id_produtos = []
nivel_par_recomendado = []
numero_fileiras = []
demanda_maxima_simulada = []
medias_consumo_df = []
desvios_consumo_df = []

for key, value in espacos.items():

    dados_cat = dados[dados['canaleta'] == key]
    limites = list(dados_cat['limite_prateleira'])
    validade = list(dados_cat['validade_baixa'])
    produto_essencial = list(dados_cat['produto_essencial'])
    fileiras_ocupadas = dados_cat['fileiras_ocupadas']
    media = list(dados_cat['media'])
    desvio = list(dados_cat['desvio'])
    espaco = value
    nivel_par_categoria = sim.otimizar_nivel_par_simples(list(media), list(desvio), 30, 2, limites,fileiras_ocupadas=fileiras_ocupadas,validades = validade, produtos_essenciais=produto_essencial,limite_espaco = espaco)
    produtos_lista.extend(list(dados_cat['produto']))
    id_produtos.extend(list(dados_cat['id_produto']))
    nivel_par_recomendado.extend(nivel_par_categoria['niveis_par'])  
    numero_fileiras.extend(nivel_par_categoria['num_fileiras'])
    demanda_maxima_simulada.extend(nivel_par_categoria['max_demanda_simulada'])
    medias_consumo_df.extend(media)
    desvios_consumo_df.extend(desvio)
planograma_df = pd.DataFrame({'id_produto':id_produtos,
                                'produto':produtos_lista,
                                'nivel_par_recomendado':nivel_par_recomendado,
                                'numero_fileiras':numero_fileiras,
                                'demanda_maxima_simulada':demanda_maxima_simulada,
                                'media_demanda':medias_consumo_df,
                                'desvio_demanda':desvios_consumo_df})
import time
tempo = time.time()
planograma_df.to_csv('./planogramas/Planograma Otimizado - Padrão' + str(tempo) + '.csv', sep = ';', decimal = ',', encoding = 'latin-1', index = False)
# %%
