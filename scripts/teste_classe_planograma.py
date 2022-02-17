#%%
# Importa pacotes e outros scripts
from pandas._libs.tslibs import Period
from classe_planograma import Ponto, importar_vendas
import vmconnection as vmpay
import pandas as pd
import numpy as np
from IPython.display import display
import warnings
import json
import otimizacao

#import new_cashless as ncash

warnings.filterwarnings('ignore')
#%%
purchases = vmpay.VMConnection().getPurchasesNewEndPoint()
#%%
# Carrega base de vendas
df_vendas = importar_vendas()
#%%
#Atualiza pontos de venda
locais = pd.DataFrame(vmpay.VMConnection().getLocations())
maquinas = pd.json_normalize(vmpay.VMConnection().getMachines())
locais = locais[['id','name']]
locais.columns = ['location_id','location_name']
maquinas = maquinas[['id','installation.location_id','installation.id']]
maquinas.columns = ['machine_id','location_id','installation_id']
locais = locais.merge(maquinas, on = ['location_id'], how='left')
locais = locais[~locais['machine_id'].isnull()]


#%%
#Cria o DataFrame 'pdvs' e muda os tipos de variáveis
pdvs = locais[['location_name','machine_id','installation_id']]
pdvs['machine_id'] = pdvs['machine_id'].astype(int)
pdvs['installation_id'] = pdvs['installation_id'].astype(int)

# Pega PDVS e gera um dicionário
#pdvs = pd.read_csv('./datasets/pontos.csv',sep = ';')[['machine_id','installation_id','location_name']]

pdvs = pdvs.drop_duplicates(subset=['machine_id','installation_id'])
#pdvs = pdvs.drop_duplicates(subset = "location_name")

### RODAR SÓ PARA MCSs
pdvs = pdvs[pdvs.location_name.str.contains('Mc |MC ')]

### RODAR SÓ PARA MMS
#pdvs = pdvs[pdvs.location_name.str.contains('Mm |MM ')]

#pdvs = pdvs[pdvs.location_name.str.contains("COSTA BLANCA")]
#pdvs = pdvs[pdvs.location_name.str.contains("torre 2")]
#pdvs = pdvs[pdvs.location_name.str.contains('GOLDEN')]
#pdvs = pdvs[0:1]
#pdvs = organiza_pdvs(pdvs)
#Lista com os nomes dos pdvs que possuem 64 espaços na cervejeira
cervejeira_bohemia = ["MC ULIVING", "Mc Porto Mare", "Mc Ocean Houses", "MC RESIDENCIAL PETRA", "MC VIVENDAS DA FREGUESIA"]
cervejeira_dupla = ["MC CARIOCA BLOCO 1", "MC CARIOCA BLOCO 2", "MC GRAGOATA BAY", "MC BEL AIR"]
refrigerante_duplo = ["MC CARIOCA BLOCO 1", "MC CARIOCA BLOCO 2", "MC GRAGOATA BAY", "MC VENTANAS NATURE RESORT", "MC Wind Residencial ", "MC MIDAS", "MC Wonderful 3", "Mc Moradas do Itanhangá"]
#Grand life icaraí refri duplo?
#BARRA TOP LIFE?
#%%
pdvs.drop_duplicates(subset = "location_name", inplace = True)
pdvs.reset_index(level = 0, drop = True, inplace = True)
#pdvs.to_csv('pdvs.csv', sep = ";", index = False)
#%%
"""Célula responsável por otimizar os planogramas dos pontos de venda selecionados"""
pontos_erro = []
lista_retornos = []
limites = pd.read_csv("./datasets/limite_prateleira.csv", sep = ";")
df_geral = pd.DataFrame()
x = 0
for i in pdvs['location_name']:
    print(i)
    try:
        #Cria o pdv
        ponto = otimizacao.cria_ponto(i)
        print(f"{x} de {len(pdvs)}")
        x += 1
        #Roda o método Cervejeira Otimizada
        #otimizacao.otimiza_cervejeira(ponto = ponto, df_vendas = df_vendas, cervejeira_dupla = cervejeira_dupla, cervejeira_bohemia = cervejeira_bohemia)
        #Método que pega um planograma
        otimizacao.get_planograma(ponto)
        planograma = ponto.planograma.reset_index(level = 0)
        planograma = planograma[(planograma.name.str.contains("CV"))]
        planograma = planograma[~((planograma.status == "suspended") & (planograma.current_balance == 0))]
        #planograma = planograma[(planograma.name.str.contains("CV"))]
        #planograma = planograma[(planograma.name.str.contains("CV")) & (planograma.status == "active") & ((planograma.status == "suspended") & planograma.current_balance > 0)] 
        c = 0
        sum = 0
        for id in planograma.good_id:
            #print(planograma.iloc[c]["par_level"] / limites[limites.id_produto == id]["limite_prateleira"])
            try:
                sum += int(planograma.iloc[c]["par_level"] / limites[limites.id_produto == id]["limite_prateleira"])
            except ValueError:
                if int(planograma.iloc[c]["par_level"] % 6) == 0:
                    sum += int(planograma.iloc[c]["par_level"] / 6)
                else:
                    sum += int(planograma.iloc[c]["par_level"] / 7)   
            c += 1
        df_geral = df_geral.append({"nome": ponto.local, "quantidade": sum}, ignore_index = True)
    except KeyError:
        print("KEY ERROR")    
#%%
lista = []
for qtd in df_geral.quantidade:
    if qtd <= 34:
        lista.append("comum")
    elif (qtd > 34) and (qtd <= 40):
        lista.append("bohemia")
    else:
        print(qtd)
        lista.append("dupla")
df_geral["classificacao"] = lista

        #ponto.salvarPlanograma()
        #ponto.salvarPlanograma()
        #Roda o método que otimiza os refrigerantes
        #otimizacao.otimiza_refrigerante(ponto = ponto, df_vendas = df_vendas, refrigerante_duplo = refrigerante_duplo)
        #Roda o método otimizar comum para as canaletas restantes
        #otimizacao.otimiza(ponto = ponto, df_vendas = df_vendas)
        #otimizacao.otimiza(ponto = ponto, df_vendas = df_vendas, espaco_otimizar = {"CC":None,"PM":None,"GB":None,"FR":None})
        #Salva os retornos numa tabela 
        #retornos = otimizacao.get_retornos(ponto)
        #retornos_cv = retornos[retornos.Canaleta.str.contains("CV")]
        #lista_retornos.append(retornos.Retorno.sum())
        #print(f"Ponto: {ponto.local} -- Retornos: {retornos_cv.Retorno.sum()}")
        #retornos.to_excel("./retornos/" + str(ponto.local) + " Retornos.xlsx", index = False)
        #Roda o método que posta o planograma otimizado
    #except:
        #pontos_erro.append(ponto.local)
    #import pdb;pdb.set_trace()
    #otimizacao.postar_planograma(ponto)

#%%
def organiza_pdvs(pdvs):
    pdvs = pdvs["location_name"].tolist()
    separador = " "
    for i in range(len(pdvs)):
        aux = pdvs[i].split()
        pdvs[i] = separador.join(aux)
    return pdvs
# %%
"""Célula Responsável por realizar o cálculo das rupturas"""
import pandas as pd 

lista_nome = []
lista_ruptura = []
j = 0
for i in pdvs['location_name']:
    #try:
    j += 1
    print("{} de {}".format(j, len(pdvs['location_name'])))
    ponto = Ponto(i)
    ponto.getPickList()
    ponto.calcularRuptura(30)
    lista_nome.append(i)
    lista_ruptura.append(ponto.rup_pct)
    print(ponto.local)
    #except:
    #    print("Deu merda no ponto " + i)    

df_ruptura = pd.DataFrame({"Nomes":lista_nome, "Ruptura":lista_ruptura})
df_ruptura.sort_values("Ruptura", ascending = False, inplace = True)

df = pd.DataFrame()
for i in lista_nome:
    aux = df_vendas[df_vendas["local"] == i]
    aux = aux[["local", "dia"]]
    aux.sort_values("dia", ascending = True, inplace = True)
    df = df.append(aux[0:1])

df.sort_values("dia", ascending = True, inplace = True)
df["dia"] = pd.to_datetime(df["dia"])

from datetime import datetime
dataref = datetime.today()
dataref = (dataref.year * 360) + (dataref.month * 30) + dataref.day

peso = []
for i in range(len(df)):
    dia = df.iloc[i,1].day
    mes = (df.iloc[i,1].month - 1) * 30
    ano = df.iloc[i,1].year * 360
    data = dataref - (ano + mes + dia)
    if data < 90 and data > 31:
        peso.append(1)
    elif data >= 90 and data < 180:
        peso.append(2)
    elif data >= 180 and data < 270:
        peso.append(3)
    else:
        peso.append(4)   

for i in range(len(df_ruptura)):
    if df_ruptura.iloc[i]["Nomes"] not in lista_nome:
        df_ruptura.drop([i], inplace = True)

df["Peso"] = peso
df.columns = ["Nomes", "Dia", "Peso"]
df_ruptura = df_ruptura.merge(df, how = "inner", on = "Nomes")
df_ruptura.sort_values(["Ruptura", "Peso"], ascending = [False, False], inplace = True)

df_ruptura = df_ruptura.drop_duplicates(subset = ['Nomes'])

#import statistics
#dframe = pd.read_excel("Rupturas.xlsx")
#desv_padrao = round(statistics.stdev(dframe.Ruptura) * 2)

peso_ruptura = []
MAIOR = df_ruptura["Ruptura"].max()
for i in df_ruptura["Ruptura"]:
    peso = round((i / MAIOR) * 10)
    peso_ruptura.append(peso)

data_hoje = datetime.today()
data_hoje = (data_hoje.year * 360) + ((data_hoje.month - 1) * 30) + data_hoje.day
data_antiga = df_ruptura["Dia"].min()
data_antiga = (data_antiga.year * 360) + ((data_antiga.month - 1) * 30) + data_antiga.day
MAIOR_DATA = data_hoje - data_antiga

peso_data = []
for i in df_ruptura["Dia"]:
    data = (i.year * 360) + ((i.month * 30) - 1) + i.day
    data = data_hoje - data
    peso = round((data / MAIOR_DATA) * 10)
    peso_data.append(peso)

peso_oficial = []
for i in range(len(peso_ruptura)):
    peso = ((peso_ruptura[i] * 2) + peso_data[i]) / 3
    peso_oficial.append(round(peso))

df_ruptura["Peso"] = peso_oficial
df_ruptura = df_ruptura[df_ruptura["Peso"] != 0]
df_ruptura.sort_values("Peso", ascending = [False], inplace = True)

df_ruptura.to_excel("Ruptura_13_09.xlsx")
# %%
"""Célula responsável por importar os arquivos contendo as rupturas dos pontos, alterar os dados necessários para o bom funcionamento do método
e, em seguida, criar a função responsável por somar as rupturas das rotas"""
import pandas as pd
df_ruptura = pd.read_excel("Ruptura_02_08.xlsx")
df_ruptura.Nomes = df_ruptura.Nomes.apply(lambda x: x.upper())
def soma_rupturas(df_rupturas, nome_rota, lista_pontos):
    soma_pesos = 0
    lista_pontos = [item.upper() for item in lista_pontos]
    for i in range(len(lista_pontos)):
        try:
            df_aux = df_rupturas[df_rupturas.Nomes.str.contains(lista_pontos[i])]
            soma_pesos += df_aux.iloc[0]["Peso"]
            print("{} -- {}".format(lista_pontos[i], df_aux.iloc[0]["Peso"]))
        except:
            print("{} -- ".format(lista_pontos[i]))
    return "Rota: {} -- Prioridade: {}".format(nome_rota, soma_pesos)

#%%
"""O Objetivo deste 'rascunho' é explorar as possibilidades analíticas do python, utilizando bibliotecas como pandas,
matplotlib e seaborn, com o intuito de, futuramente, criar uma classe em python que trabalhe com os dados fornecidos pela
vmpay da forma mais autônoma possível, facilitando as análises e a geração de insights"""
# Importa pacotes e outros scripts
from pandas._libs.tslibs import Period
from classe_planograma import Ponto, importar_vendas
import vmconnection as vmpay
import pandas as pd
import numpy as np
from IPython.display import display
import warnings
import json

warnings.filterwarnings('ignore')

# Carrega base de vendas
df_vendas = importar_vendas(only_mc=False)

# Importa a biblioteca de datas e cria uma variável que armazena a data do momento em que o código é executado
from datetime import datetime
data = datetime.today()

# Seleciona somente as vendas do ano atual
vendas_2021 = df_vendas[df_vendas.data.dt.year == data.year]

# Remove os valores NaN, sendo necessário apenas para a utilização do método 'str.contains()'
#vendas_2021.dropna(inplace = True)
# Pega os produtos que tem algumas das palavras mencionadas em seu nome
#vendas_sorvete = vendas_2021[vendas_2021.produto.str.contains("Kibon | kibon | Ben | ben | Cornetto | cornetto")]
# Contando a qtd de produtos vendidos no ano de 2021
#vendas_2021.produto.value_counts().reset_index()
# Verifica a quantidade de máquinas que realizaram ao menos uma venda em 2021
#vendas_2021["machine_id"].groupby(vendas_2021["data"].dt.month).describe()
# Pega os 15 produtos mais vendidos do ano
#top_produtos = vendas_2021.produto.value_counts().reset_index().head(15)

# Pega os 15 produtos mais vendidos do último mês
vendas_mes = vendas_2021[vendas_2021.data.dt.month == data.month - 1]
top_produtos = vendas_mes.produto.value_counts().reset_index().head(15)
colunas = ["nome", "id_produto"]
top_produtos.columns = colunas

#Pega os 15 produtos mais vendidos do mês retrasado
vendas_mes_comparativo = vendas_2021[vendas_2021.data.dt.month == data.month - 2]
top_produtos_comparativo = vendas_mes_comparativo.produto.value_counts().reset_index().head(15)
colunas = ["nome", "id_produto"]
top_produtos_comparativo.columns = colunas

# Analisa e mostra quantas posições determinado produto subiu ou desceu na lista
produtos_atuais = top_produtos.nome.tolist()
posicao = []
for i in range(len(produtos_atuais)):
    if produtos_atuais[i] in top_produtos_comparativo.nome.tolist():
        #Posição do item no mês retrasado
        mes_x = top_produtos_comparativo[top_produtos_comparativo.nome == produtos_atuais[i]].index
        #Posição do item no mês passado
        mes_y = top_produtos[top_produtos.nome == produtos_atuais[i]].index
        posicao.append(mes_x - mes_y)
    else:
        posicao.append([0])
top_produtos["posicao"] = posicao
posicao = top_produtos["posicao"].tolist()
nova_posicao = []
for i in posicao:
    try:
        #print(i.tolist()[0])
        nova_posicao.append(i.tolist()[0])
    except:
        #print("Zero")
        nova_posicao.append(0)
top_produtos["posicao"] = nova_posicao

# Plota e salva os gráficos das vendas por mês, em quantidade, dos 15 produtos mais vendidos
# Também plota um mapa de calor mostrando a correlação entre a quantidade de pdvs que ofertam o produto e a quantidade de vendas
# Salva o dataframe dos produtos mais vendidos em um arquivo Excel
import matplotlib.pyplot as plt
import seaborn as sns

for i in range(len(top_produtos)):
    # Pega as vendas do ano até o mês anterior
    vendas_produto = vendas_2021[(vendas_2021["produto"] == top_produtos.iloc[i]["nome"]) & (vendas_2021.data.dt.month != data.month)]
    fig, axes = plt.subplots()
    vendas_produto.groupby(vendas_produto["data"].dt.month)["requisicao"].count().plot.line(ax = axes)
    axes.set_title(top_produtos.iloc[i]["nome"])
    axes.set_xlabel("Mês")
    sns.set()
    plt.savefig("graficos/" + top_produtos.iloc[i]["nome"] + ".png")
    plt.show()
    describe_2021 = vendas_produto["machine_id"].groupby(vendas_produto["data"].dt.month).describe()
    col_maquinas = describe_2021["unique"].tolist()
    col_vendas = vendas_produto.groupby(vendas_produto["data"].dt.month)["requisicao"].count().tolist()
    df_corr = pd.DataFrame({"Num Maquinas":col_maquinas, "Vendas":col_vendas})
    sns.heatmap(df_corr.corr(), annot = True)
    plt.savefig("graficos/" + top_produtos.iloc[i]["nome"] + " Correlação" + ".png")
    plt.show()

top_produtos.to_excel("Mais vendidos 0" + str(data.month - 1) + "-" + str(data.year) + ".xlsx")

# %%
import discord
import requests
import traceback

from datetime import datetime
from discord import Webhook, RequestsWebhookAdapter
from django_currentuser.middleware import get_current_user

DISCORD_LINK = "https://discord.com/api/webhooks/862556688316760064/hqgH1s2wrTX91genpFxlF6SLwUbp5Bmv4Jvkve4fYt5dm41BBk-Q-S2J5dDERuVpEDpS"

def send_traceback(username, trace):
      IP = requests.get('https://api.ipify.org').text
#%%
#CORRELAÇÃO VENDAS X TEMPERATURA
meteoro = pd.read_csv("meteoro.csv", sep = ";")
meteoro.temperatura_media_diaria = meteoro.temperatura_media_diaria.apply(lambda x: x.replace(",","."))
meteoro.temperatura_media_diaria = meteoro.temperatura_media_diaria.astype("int")
meteoro.data_medicao = meteoro.data_medicao.apply(lambda x: datetime.strptime(str(x), '%d/%m/%Y'))
vendas_2021 = df_vendas[df_vendas.data.dt.year == 2021]
vendas_2021 = vendas_2021[(vendas_2021.data.month < 8)]
vendas_chocolate = vendas_2021.dropna()
vendas_chocolate = vendas_chocolate[vendas_chocolate.produto.str.contains("Chocolate | chocolate | CHOCOLATE")]
lista = [1,2,3,4,5,6,7]
c_vendas = []
for i in lista:
    aux = vendas_chocolate[vendas_chocolate.data.dt.month == i]
    c_vendas.append(aux.groupby(aux["data"].dt.day)["produto"].count().tolist())
lista_vendas = []
for i in c_vendas:
    for j in i:
        lista_vendas.append(j)
meteoro["vendas_dia"] = lista_vendas
df = meteoro[["temperatura_media_diaria", "vendas_dia"]]
df.corr()
#%%
import math
def get_cobertura_estoque(vendas):
    #import pdb;pdb.set_trace()
    vendas_merge = vendas
    vendas_merge["vendas_dias"] = vendas_merge["requisicao"] / 30
    vendas_merge["cobertura_estoque"] = vendas_merge["par_level"] / vendas_merge["vendas_dias"]
    vendas_merge.loc[vendas_merge["cobertura_estoque"] == math.inf, "cobertura_estoque"] = 90
    vendas_merge.fillna(value = {"cobertura_estoque" : 90}, inplace = True)
    return vendas_merge.cobertura_estoque.mean()
# %%
from classe_planograma import Ponto, importar_vendas
import otimizacao
import datetime
import pandas as pd
#pdvs = pd.read_csv("pdvs.csv", sep = ";")
#pdvs = pdvs[30:33]
#print(pdvs.tail(25))
df_vendas = importar_vendas(only_mc=False, from_api = True)
lista_coberturas = []
lista_erros = []
lista_pdvs = []
for nome in pdvs.location_name:
    try:
        #Cria o pdv e puxa seu planograma
        ponto = otimizacao.cria_ponto(nome)
        planograma = otimizacao.get_planograma(ponto)[0]
        #Pega o DataFrame das Vendas dos últimos 30 dias
        vendas = df_vendas[df_vendas.local == ponto.local]
        vendas = vendas[vendas["dia"] > (max(vendas["dia"]) - datetime.timedelta(days = 30))]
        planograma.reset_index(level = 0, inplace = True)
        planograma = planograma[(planograma.type == "Coil") & (planograma.status == "active")]
        planograma = planograma[~planograma.name.str.contains("SV")]
        planograma = planograma[["good_id", "par_level"]]
        #Agrupa as vendas por id do produto e faz as manipulações necessárias
        vendas_agrupadas = vendas.groupby("id_produto").count()
        vendas_agrupadas.reset_index(level = 0, inplace = True)
        vendas_agrupadas = vendas_agrupadas[["id_produto", "requisicao"]]
        vendas_agrupadas = vendas_agrupadas.rename(columns = {"id_produto": "good_id"})
        vendas_agrupadas["good_id"] = vendas_agrupadas["good_id"].astype(int)
        vendas_agrupadas.sort_values(by = "requisicao", ascending = False, inplace = True)
        vendas_merge = vendas_agrupadas.merge(planograma, "outer", "good_id")
        lista_coberturas.append(get_cobertura_estoque(vendas_merge))
        print(ponto.local)
        lista_pdvs.append(ponto.local)
    except:
        print("Deu merda com o {}".format(ponto.local))    
        lista_erros.append(ponto.local)
df_cobertura = pd.DataFrame({"ponto": lista_pdvs, "cobertura": lista_coberturas})
#%%
import otimizacao
import datetime
import vmconnection as vmpay
def get_planograma_data(ponto, data, i = -1):
    plan = vmpay.VMConnection().getPlanogram(ponto.machine_id,ponto.installation_id)[i]
    plan = plan["updated_at"]
    plan_date = datetime.datetime.strptime(plan[0:10],'%Y-%m-%d').date()
    while plan_date > data:
        i -= 1
        plan = vmpay.VMConnection().getPlanogram(ponto.machine_id,ponto.installation_id)[i]
        plan = plan["updated_at"]
        plan_date = datetime.datetime.strptime(plan[0:10],'%Y-%m-%d').date()    
        print("Data Desejada: {} --- Data Atual: {}".format(data, plan_date))
    plano = otimizacao.get_planograma(ponto, i)[0]
    return plano
# %%
from classe_planograma import Ponto, importar_vendas
import otimizacao
import datetime
import pandas as pd
#pega o df das vendas
df_vendas = importar_vendas(only_mc=False, from_api = True)
ponto = otimizacao.cria_ponto("MC Wonderful 1")
#data que eu desejo
data = datetime.datetime.strptime('03-08-2021','%d-%m-%Y').date()
vendas = df_vendas[df_vendas.local == ponto.local]
vendas = vendas[vendas["dia"] > (data - datetime.timedelta(days = 30))]
planograma = get_planograma_data(ponto, data)
planograma.reset_index(level = 0, inplace = True)
planograma = planograma[(planograma.type == "Coil") & (planograma.status == "active")]
planograma = planograma[~planograma.name.str.contains("SV")]
planograma = planograma[["good_id", "par_level"]]
#Agrupa as vendas por id do produto e faz as manipulações necessárias
vendas_agrupadas = vendas.groupby("id_produto").count()
vendas_agrupadas.reset_index(level = 0, inplace = True)
vendas_agrupadas = vendas_agrupadas[["id_produto", "requisicao"]]
vendas_agrupadas = vendas_agrupadas.rename(columns = {"id_produto": "good_id"})
vendas_agrupadas["good_id"] = vendas_agrupadas["good_id"].astype(int)
vendas_agrupadas.sort_values(by = "requisicao", ascending = False, inplace = True)
vendas_merge = vendas_agrupadas.merge(planograma, "outer", "good_id")
cb = get_cobertura_estoque(vendas_merge)
print(cb)
# %%
import otimizacao
import pandas as pd
#Passo 1 - Ler a tabela contendo os retornos
retornos = pd.read_excel(r"./datasets/Retornos.xlsx")
#Passo 2 - Ler a tabela pivotada
tabela_pivotada = pd.read_excel(r"dataframe_pivotado.xlsx")
import otimizacao
#Passo 3 - Criar um novo dataframe contendo apenas as 5 primeiras linhas do Dataframe 'tabela_pivotada' após o mesmo ser reorganizado de acordo com a quantidade do produto selecionado
planograma = pd.DataFrame()
df_geral = pd.DataFrame()
pontos_erro = []
for i in range(len(retornos)):
    demanda = tabela_pivotada.sort_values(by = retornos.iloc[i]["Produto"], ascending = False).head()
    for pdv in demanda.local:#loop que pega os planogramas dos 5 pontos de vendas presentes no df 'demanda'
        ponto = otimizacao.cria_ponto(pdv)
        print(ponto.local)
        dados_get_planograma = otimizacao.get_planograma(ponto)
        planograma_aux = dados_get_planograma[0]
        planograma_aux["machine_id"] = dados_get_planograma[2]
        planograma_aux["installation_id"] = dados_get_planograma[3]
        planograma_aux["planograma_id"] = dados_get_planograma[4]
        planograma_aux = planograma_aux[planograma_aux.name == retornos.iloc[i]["Canaleta"]]
        planograma_aux.reset_index(level = 0, inplace = True)
        planograma_aux["ponto"] = ponto.local
        planograma_aux = planograma_aux[["id", "good_id", "name", "par_level", "current_balance", "machine_id", "installation_id", "planograma_id"]]
        planograma = pd.concat([planograma, planograma_aux])
    planograma["demanda"] = planograma["par_level"] - planograma["current_balance"]
    planograma.sort_values(by = "demanda", ascending = False, inplace = True)
    df_geral = pd.concat([df_geral, planograma[0:1]])
    planograma = pd.DataFrame()
    print(df_geral)
#Passo 4 - Passar o dataframe geral novamente no banco de dados contendo os retornos, desta vez para retirar as quantidades possíveis de cada produto
lista_qtd = []
lista_produto = []
for i in range(len(retornos)):
    index_produto = retornos[retornos.Canaleta == df_geral.iloc[i]["name"]].index
    index_produto = index_produto[0]
    produto = retornos.iloc[index_produto]["Produto"]
    retorno = retornos.iloc[index_produto]["Retorno"]
    qtd_planograma = int(df_geral.iloc[i]["demanda"])
    if retorno - qtd_planograma >= 0:
        sub = retorno - qtd_planograma
        retornos.at[index_produto, "Retorno"] = int(sub)
        quantidade = qtd_planograma
    else:
        quantidade = retorno
        retornos.at[index_produto, "Retorno"] = 0         
    lista_qtd.append(quantidade)
    lista_produto.append(produto)
df_geral["demanda_possivel"] = lista_qtd
  
# %%
def createPicklist(self, machine, inst, planogram_id, items_attributes):
    machine = str(machine)
    inst = str(inst)
    picklist = {"pick_list": {
                    "planogram_id": planogram_id,
                    "items_attributes": items_attributes#lista de dicionarios com item_id e quantidade
                }
                }
    URL = 'machines/' + str(machine) + '/installations/' + str(inst) + '/pick_lists'
    response = requests.post(self.BASEURL % URL , params=self.PAYLOAD, data=json.dumps(picklist), headers=self.HEADERS)
    if not response.ok:
        return None
    return response.content

#%%
df_cerveja = df_vendas[(df_vendas.produto.str.contains("Cerveja")) | (df_vendas.produto.str.contains("Skol")) | (df_vendas.produto.str.contains("Vinho"))]
df_cerveja = df_cerveja[df_cerveja.local.str.contains("MC")]
data_hoje = datetime.datetime.strptime('25-10-2021','%d-%m-%Y').date()
df_cerveja = df_cerveja[df_cerveja["dia"] > (data_hoje - datetime.timedelta(days = 1))]
df_agrupado = df_cerveja.groupby(by = ["local"])["requisicao"].count().reset_index(level = 0)
df_agrupado = df_agrupado.reset_index(level = 0, drop = True)
df_pivotado = df_agrupado.pivot(index = "local", columns = "produto")
df_pivotado.fillna(0, inplace = True)
# %%
""" I - Processo de Volta
    Iremos escolher um pdv de uma das duas rotas de inspeção e ler os arquivos da pasta de retornos,
    selecionando quais cervejas podem ser retiradas dos pdvs """
import os
import pandas as pd
path = "./retornos"
freezer = 150
cooler = 80
for j in range(len(os.listdir(path))):
    nome = os.listdir(path)[j]
    print(nome)
    ponto = pd.read_excel("./retornos/" + nome)
    ponto = ponto[ponto.Canaleta.str.contains("CV")]
    df_suspensos = ponto[ponto.Recomendado == 0]
    df_ativos = ponto[ponto.Recomendado != 0]
    df_suspensos = df_suspensos[df_suspensos.columns[1:]]
    df_ativos = df_ativos[df_ativos.columns[1:]]
    df_suspensos.reset_index(level = 0, drop = True, inplace = True)
    df_ativos.reset_index(level = 0, drop = True, inplace = True)
    i = 0

    if df_suspensos.Retorno.sum() < cooler:
        #import pdb;pdb.set_trace()
        df_cooler = df_suspensos#[["Canaleta", "Produto", "Retorno"]]
        cooler -= df_suspensos.Retorno.sum()
        while cooler > 0:
            #import pdb;pdb.set_trace()
            try:
                if df_ativos.iloc[i]["Retorno"] < cooler:
                    cooler -= df_ativos.iloc[i]["Retorno"]
                    df_cooler = pd.concat([df_cooler, df_ativos[i:i+1]], ignore_index = True)
                else:
                    df_aux = df_ativos[i: i+1]
                    df_aux.at[i, "Retorno"] = cooler
                    df_cooler = pd.concat([df_cooler, df_aux], ignore_index = True)
                    cooler = 0
                i += 1
            except IndexError:
                print("Todas as Cervejas Foram Alocadas")
                df_cooler = df_cooler[df_cooler.Retorno != 0]
                df_cooler.to_excel("./voltas/" + nome + "_volta.xlsx", index = False)
                break
        df_cooler = df_cooler[df_cooler.Retorno != 0]
        df_cooler.to_excel("./voltas/" + nome + "_volta.xlsx", index = False)
    else:
        df_cooler = pd.DataFrame()
        while cooler > 0:
            if df_suspensos.iloc[i]["Retorno"] < cooler:
                cooler -= df_suspensos.iloc[i]["Retorno"]
                df_cooler = pd.concat([df_cooler, df_suspensos[i:i+1]], ignore_index = True)
            else:
                df_aux = df_suspensos[i: i+1]
                df_aux.at[i, "Retorno"] = cooler
                df_cooler = pd.concat([df_cooler, df_aux], ignore_index = True)
                cooler = 0
            i += 1
        #df_cooler = df_cooler[df_cooler.Retorno != 0]
        df_cooler.to_excel("./voltas/" + nome + "_volta.xlsx", index = False)
    if cooler < 7:
        print("limite atingido")
        break
# %%
"""Trata o df das cervejas"""
import datetime
df_cerveja = df_vendas[(df_vendas.produto.str.contains("Cerveja")) | (df_vendas.produto.str.contains("Skol")) | (df_vendas.produto.str.contains("Vinho"))]
df_cerveja = df_cerveja[df_cerveja.local.str.contains("MC")]
data_hoje = datetime.datetime.strptime('06-10-2021','%d-%m-%Y').date()
df_cerveja = df_cerveja[df_cerveja["dia"] == data_hoje]
df_agrupado = df_cerveja.groupby(by = ["local"])["valor"].sum().reset_index(level = 0)
df_agrupado.sort_values("valor", ascending = False, inplace = True)
#%%
import pandas as pd
rotas = pd.read_excel("rotas13.xlsx", sheet_name = "Contagem")
rotas = rotas[rotas.columns[1:8]]
rotas.dropna(how = "all", inplace = True)
rotas.columns = ["c1", "c2", "c3", "c4", "c5", "c6", "c7"]
rotas.reset_index(level = 0, drop = True, inplace = True)
index = rotas[rotas["c1"] == "JHON E BRUNO"].index[0]
rota_1 = rotas[0: index]
rota_2 = rotas[index:]
# %%
#ADICIONANDO PRODUTOS AO BANCO DE DADOS DE RETORNO
for linha in range(len(retorno)):
    index = banco_retornos[banco_retornos.Canaleta == retorno.iloc[linha]["Canaleta"]].index[0]
    banco_retornos.at[index, "Retorno"] = retorno.iloc[linha]["Retorno"]
#%%
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

#%%
import pandas as pd
import otimizacao
from datetime import datetime
pontos_errados = []
planograma_definitivo = pd.DataFrame()
pdvs = pd.read_csv("pdvs.csv", sep = ";")
produtos = pd.read_csv("produtos.csv", sep = ";")
produtos = produtos[["id", "name", "upc_code"]]
produtos.rename(columns = {"id": "good_id", "name": "produto"}, inplace = True)
c = 1
for pdv in pdvs.location_name:
    print("{} de 119".format(c))
    c += 1
    #try:
    ponto = otimizacao.cria_ponto(pdv)
    nome = ponto.local
    machine_id = ponto.machine_id
    installation_id = ponto.installation_id
    otimizacao.get_planograma(ponto)
    planograma = ponto.planograma[ponto.planograma.type == "Coil"]
    planograma = planograma[~((planograma.status == "suspended") & (planograma.current_balance == 0))]
    planograma = planograma[["id", "name", "par_level", "current_balance"]]
    planograma["local"] = nome
    planograma["installation_id"] = installation_id
    planograma["machine_id"] = machine_id
    planograma.reset_index(level = 0, inplace = True) 
    planograma = planograma.merge(produtos, on = "good_id", how = "left")
    planograma_definitivo = pd.concat([planograma_definitivo, planograma])
    #except:
    pontos_errados.append(ponto.local)

planograma_definitivo.reset_index(level = 0, drop = True, inplace = True)

# %%
#SELEÇÃO DE PONTOS DAS ROTAS DE ABASTECIMENTO
import datetime
#filtrar por coil
df_cerveja = df_vendas[(df_vendas.produto.str.contains("Cerveja")) | (df_vendas.produto.str.contains("Skol")) | (df_vendas.produto.str.contains("Vinho"))]
df_cerveja = df_cerveja[df_cerveja.local.str.contains("MC")]
#datetime.now()
data_ontem = datetime.datetime.strptime('15-02-2022','%d-%m-%Y').date()
df_cerveja = df_cerveja[df_cerveja["dia"] == data_ontem]
df_agrupado = df_cerveja.groupby(by = ["local"])["valor"].sum().reset_index(level = 0)
df_agrupado.sort_values("valor", ascending = True, inplace = True)

rota = pd.DataFrame()
for df in os.listdir("./rotas"):
    df = pd.read_csv("./rotas/" + df)
    display(df)
    rota = rota.append(df)

lista_colunas = []
for coluna in rota.columns:
    lista_colunas.append(rota[coluna].tolist())

def flatten(t):
    return [item for sublist in t for item in sublist]

#lista_colunas = flatten(lista_colunas)
lista_colunas_2 = [" ".join(lista_colunas[i].split()).upper() for i in range(len(lista_colunas))]
#df_agrupado = df_agrupado[df_agrupado.local.isin(lista_colunas)]
df_agrupado = df_agrupado[(df_agrupado.local.isin(lista_colunas)) | (df_agrupado.local.isin(lista_colunas_2))]

# %%
#CRIAÇÃO DOS PLANOGRAMAS DOS PONTOS DE ABASTECIMENTO SELECIONADOS
for i in df_agrupado['local']:
    try:
        ponto = otimizacao.cria_ponto(i)
        otimizacao.get_planograma(ponto)
    except KeyError:
        print("Planograma Pendente ou Nome errado")
        continue
    ponto.planograma.to_csv('../datasets/planogramas/retornos_planograma/Planograma ' + ponto.local + '.csv', sep = ';', decimal = ',', encoding = 'latin-1')
# %%
