#%%
from vmconnection import VMConnection
import pandas as pd
import numpy as np

vm = VMConnection()
print('ok')

# %%
#Carrega a tabela com os novos produtos adicionados
produtos_adicionados = pd.read_excel('./datasets/Controle de Produtos Adicionados_V1.xlsx',sheet_name = 'Produtos')
#Remove os produtos nulos do dataframe 
produtos_adicionados = produtos_adicionados[~produtos_adicionados['Produto'].isnull()]
#Transforma o tipo de dados para inteiro
produtos_adicionados['Código'] = produtos_adicionados['Código'].astype(int)

produtos = pd.DataFrame(vm.getProducts())
produtos['upc_code'] = produtos['upc_code'].astype(int)

#Carrega a tabela com as modificações dos limites das prateleiras
limite = pd.read_csv('./datasets/limite_prateleira.csv', sep = ';', decimal = ',')
#Muda o tipo de dados para inteiro
limite['id_produto'] = limite['id_produto'].astype(int)
#Transforma a chave id_produto no index das linhas
limite.set_index('id_produto', inplace=True)

produtos_adicionados = produtos_adicionados.merge(produtos, left_on = ['Código'], right_on = ['upc_code'], how = 'left')

produtos_adicionados = produtos_adicionados[['id','name','Lim. Prateleira','Validade','Dt. Inclusão']]

produtos_adicionados['Validade'] = [1 if (produtos_adicionados.loc[i,'Validade'] == 'Baixa') | (produtos_adicionados.loc[i,'Validade'] == 'Muito Baixa') else np.nan for i in produtos_adicionados.index]
produtos_adicionados['Dt. Inclusão'] = produtos_adicionados['Dt. Inclusão'].dt.strftime("%d/%m/%Y")

#produtos_adicionados = produtos_adicionados[['id','name','Limite Prateleira','validade_baixa','data_inclusao']]
#Muda o nome das colunas
produtos_adicionados.columns = ['id_produto','Produto','limite_prateleira','validade_baixa','data_inclusao']
#Remove os ids nulos
produtos_adicionados = produtos_adicionados[~produtos_adicionados['id_produto'].isnull()] 
#Converte para inteiro
produtos_adicionados['id_produto'] = produtos_adicionados['id_produto'].astype(int)
#Seleciona somente os produtos que estão no DataFrame limite
produtos_alterados = produtos_adicionados[produtos_adicionados['id_produto'].isin(list(limite.index))]
produtos_alterados.set_index('id_produto', inplace=True)

produtos_adicionados = produtos_adicionados[~produtos_adicionados['id_produto'].isin(list(limite.index))]#pedro
produtos_adicionados.set_index('id_produto', inplace=True)#pedro

limite = pd.concat([limite,produtos_adicionados])#pedro

limite.update(produtos_alterados)

limite.to_csv('./datasets/limite_prateleira.csv', sep=';',decimal=',')

#%%
