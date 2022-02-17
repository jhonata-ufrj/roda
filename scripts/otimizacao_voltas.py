# %%
""" I - Processo de Volta
    Iremos escolher um pdv de uma das duas rotas de inspeção e ler os arquivos da pasta de retornos,
    selecionando quais cervejas podem ser retiradas dos pdvs """
import os
import pandas as pd
import retornos as rt
import vmconnection as vmpay
from pathlib import Path
import requests
import json
# %%
""" II - Processo de abastecimento
    Neste processo iremos atualizar o banco de dados do freezer e escoar as cervejas para a rota escolhida """

#Seleciona somente as cervejas do banco de dados
#rt.banco_retornos = pd.DataFrame()
#rt.banco_retornos = pd.read_excel(r"../datasets/retornos.xlsx")
#rt.banco_retornos = rt.banco_retornos[["Canaleta", "Produto", "Retorno"]]
#%%
#Cria os caminhos para as pastas que irão armazenar os arquivos caso elas não existam
caminho_retornos = r"../datasets/retornos_database"
caminho_planogramas = r"../datasets/planogramas/retornos_planograma"
Path(caminho_retornos).mkdir(parents=True, exist_ok=True)
Path(caminho_planogramas).mkdir(parents=True, exist_ok=True)
#Pega a lista dos retornos existentes na pasta do banco de dados
lista_retornos = os.listdir(caminho_retornos)#Adicionar código para criar diretório
#Pega a lista dos planogramas existentes na pasta
lista_planogramas = os.listdir(caminho_planogramas)#Adicionar código para criar diretório

#Pega a lista dos produtos cadastrados na vmpay
produtos = vmpay.VMConnection().getProducts()
produtos = pd.json_normalize(produtos)
produtos = produtos.rename(columns = {"id": "good_id"})
produtos = produtos[["good_id", "name"]]
pdvs = pd.read_csv("../datasets/otimizacao/pdvs.csv", sep = ";")

#Cria o banco de dados com base nos retornos
for j in range(len(lista_retornos)):
    planograma = pd.read_excel("../datasets/retornos_database/" + lista_retornos[j])
    planograma = rt.transforma_retorno(planograma)
    join = os.path.join(r"../datasets/retornos_database", lista_retornos[j])#Cria um caminho(para o arquivo que está sendo lido) para ser passado como parâmetro
    os.remove(join)#Remove o arquivo do caminho criado; Neste caso o objetivo é ir lendo e removendo os arquivos da pasta do banco de dados
    for i in range(len(planograma)):
        #baixar do form de voltas
        rt.banco_retornos = rt.atualiza_banco(planograma, i, banco_retornos = rt.banco_retornos)
#%%
#Cria as listas que receberão os 'machine_id' e 'installation_id' dos pontos de venda
###ATUALIZAR A LISTA DE PDVS
pdvs = pd.read_csv("../datasets/otimizacao/pdvs.csv", sep = ";")
pdv_machine_id = []
pdv_installation_id = []
for x in range(len(lista_planogramas)):
    lista = lista_planogramas[x].split(sep = ".")
    lista = lista[0].split(sep = " ")
    lista = lista[1:]
    lista = " ".join(lista)
    print(lista)
    aux = pdvs[pdvs.location_name.str.contains(lista)]
    pdv_machine_id.append(aux.iloc[0,1])
    pdv_installation_id.append(aux.iloc[0,2])

#%%
lista_picklist = []
lista_qtd = []
df_loop = rt.banco_retornos.copy()
for n in range(len(lista_planogramas)):
    print(lista_planogramas[n], n)
    rt.banco_retornos = df_loop.copy()
    df_picklist = pd.DataFrame()
    #Lê o planograma da lista
    planograma = pd.read_csv("../datasets/planogramas/retornos_planograma/" + lista_planogramas[n], sep = ";")
    #Seleciona somente as cervejas do planograma
    planograma = rt.transforma_planograma(planograma)
    planograma["machine_id"] = pdv_machine_id[n]
    planograma["installation_id"] = pdv_installation_id[n]
    #Pega os produtos existentes e adiciona à coluna do planograma
    produtos["good_id"] = produtos["good_id"].astype("int")
    planograma = planograma.merge(produtos, how = "inner", on = "good_id")
    #Pega o id do Planograma
    plan = vmpay.VMConnection().getPlanogram(pdv_machine_id[n],pdv_installation_id[n])[-1]
    plan_id = plan['id']
    items_id = pd.json_normalize(plan["items"])
    
    #Pega todos os Ids e Nomes dos produtos // Somente os Ids estão sendo utilizados
    item_name = []
    item_qtd = []
    for i in range(len(planograma)):
        item = rt.retorno_to_picklist(planograma, i)
        item_name.append(item[0])
        item_qtd.append(item[1])
    planograma["envio"] = item_qtd
    planograma["plan_id"] = plan_id
    #Adiciona este planograma ao planograma final
    df_picklist = df_picklist.append(planograma)
    #Adiciona os item_id aos produtos existentes do dataframe
    try:
        df_picklist["item_id"] = items_id[items_id.good_id.isin(df_picklist.good_id.tolist())]["id"].tolist()
    except:
        continue
    aux = pd.DataFrame()
    limite_cooler = 20
    for i in range(len(df_picklist)):
        if limite_cooler - df_picklist.iloc[i]["envio"] > 0:
            limite_cooler -= df_picklist.iloc[i]["envio"]
            aux = aux.append(df_picklist.iloc[i:i+1])
        else:
            linha = df_picklist[i : i+1].reset_index(level = 0, drop = True)
            linha.at[0, "envio"] = limite_cooler
            aux = aux.append(linha)
            limite_cooler -= df_picklist.iloc[i]["envio"]
            break
    display(df_picklist[df_picklist.envio>0])
    lista_qtd.append(df_picklist.envio.sum())
    lista_picklist.append([df_picklist[df_picklist.envio>0], rt.banco_retornos])
df_qtd = pd.DataFrame({"ponto": lista_planogramas, "quantidade": lista_qtd})
df_qtd.sort_values(by = "quantidade", ascending = False, inplace = True)
display(df_qtd)
print("OK")
#%%
""" III - Processo de criação de picklist
    Neste processo o programa percorre o planograma com as informações dos produtos que estão no freezer e vão para a rota e 
    coletam os dados necessários para a criação da picklist"""
n = int(input("Digite o Index da picklist desejada: "))
planograma_id = lista_picklist[n][0].iloc[0]["plan_id"]
machine_id = lista_picklist[n][0].iloc[0]["machine_id"]
installation_id = lista_picklist[n][0].iloc[0]["installation_id"]
item_attributes= []
for i in range(len(lista_picklist[n][0])):
    planogram_item_id = str(lista_picklist[n][0].iloc[i]["item_id"])
    quantidade = str(lista_picklist[n][0].iloc[i]["envio"])
    item_attributes.append({"planogram_item_id": str(planogram_item_id),
                            "quantity": str(quantidade)})
#%%
#Célula de criação das picklists sem a necessidade de criar um objeto 
BASEURL = 'http://vmpay.vertitecnologia.com.br/api/v1/%s'
PAYLOAD = { "access_token": "X6WktVwPeKagMHZh2iBBoEdK2cZK3h3TWQN5ZDDT" }
HEADERS = { 'content-type': 'application/json; charset=utf-8' }
def createPicklist(machine, inst, planogram_id, items_attributes):
    machine = str(machine)
    inst = str(inst)
    picklist = {"pick_list": {
                    "planogram_id": planogram_id,
                    "items_attributes": items_attributes#lista de dicionarios com item_id e quantidade
                }
                }
    URL = 'machines/' + str(machine) + '/installations/' + str(inst) + '/pick_lists'
    response = requests.post(BASEURL % URL , params=PAYLOAD, data=json.dumps(picklist), headers=HEADERS)
    if not response.ok:
        return None
    return response.content
# %%
createPicklist(str(machine_id), str(installation_id), str(planograma_id), item_attributes)
rt.salva_retorno(lista_picklist[n][1])