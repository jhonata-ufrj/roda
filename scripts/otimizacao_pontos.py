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
from datetime import datetime
warnings.filterwarnings('ignore')
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
### Cria o DataFrame 'pdvs' e muda os tipos de variáveis
pdvs = locais[['location_name','machine_id','installation_id']]
pdvs['machine_id'] = pdvs['machine_id'].astype(int)
pdvs['installation_id'] = pdvs['installation_id'].astype(int)
pdvs = pdvs.drop_duplicates(subset=['machine_id','installation_id'])
### RODAR SÓ PARA MCS
#pdvs = pdvs[pdvs.location_name.str.contains('Mc |MC ')]
### RODAR SÓ PARA MMS
#pdvs = pdvs[pdvs.location_name.str.contains('Mm |MM ')]
### RODAR SÓ PARA PONTO ESPECÍFICO
pdvs = pdvs[pdvs.location_name.str.contains("MC VIVENDAS DA PENHA")]
### Lista com os nomes dos pdvs que possuem 64 espaços na cervejeira
cervejeira_bohemia = ["MC ULIVING", "MC PORTO MARE", "MC OCEAN HOUSES", "MC RESIDENCIAL PETRA", "MC VIVENDAS DA FREGUESIA"]
cervejeira_dupla = ["MC CARIOCA BLOCO 1", "MC CARIOCA BLOCO 2", "MC GRAGOATA", "MC BEL AIR", "MC SPAZIO REDENTORE", "MC BARRA GOLDEN"]
refrigerante_duplo = ["MC CARIOCA BLOCO 1", "MC CARIOCA BLOCO 2", "MC GRAGOATA", "MC VENTANAS NATURE RESORT", "MC WIND RESIDENCIAL", "MC MIDAS", "MC WONDERFUL 3", "MC MORADAS DO ITANHANGA", "MC BEL AIR", "MC GRAND LIFE ICARAI", "MC BARRA GOLDEN"]
#%%
def espaco_final(s):
    s_splited = s.split()
    s_splited.append("")
    return " ".join(s_splited)
#%%
#IGNORAR AS CÉLULAS VERMELHAS
#LER O HIST OTIMIZ E ~ ISIN 
#ADICIONAR LOCATION ID
#PEGAR DO HISTORICO LOCAL
lista_pontos_mc = [
"MC MARAPENDI",
"MC VILLAGE GARDEN",
"MC ANDREA",
"MC HOME AWAYS",
"MC CARIOCA BLOCO 1",
"MC MIRANTE 5 ESTRELAS",
"MC VIVENDAS DA PENHA",
"MC PLAZA MAYOR",
"MC OCEANFRONT"
]
lista_pontos_mm = [
"MM Datacenter",
"STONE 5 ANDAR (TORRE 2)",
"MM GLOBO SUMARÉ",
"MM BASILIO ADVOGADOS"
]
lista_pontos_2 = [" ".join(lista_pontos_mm[i].split()).upper() for i in range(len(lista_pontos_mm))]
lista_pontos_3 = [espaco_final(i) for i in lista_pontos_2]
#%%
pdvs = pdvs[pdvs.location_name.isin(lista_pontos_mm)]
pdvs = pdvs[(pdvs.location_name.isin(lista_pontos_2)) | (pdvs.location_name.isin(lista_pontos_3))]
pdvs.drop_duplicates(subset = "location_name", inplace = True)
pdvs.reset_index(level = 0, drop = True, inplace = True)

#%%
"""Célula responsável por otimizar os planogramas dos pontos de venda residenciais(MC)"""
pontos_erro = []
lista_retornos = []
for i in pdvs.location_name:
    try:
        #Cria o ponto de venda(pdv)
        ponto = otimizacao.cria_ponto(i)
        #Roda o método Cervejeira Otimizada
        otimizacao.otimiza_cervejeira(ponto=ponto, df_vendas=df_vendas, cervejeira_dupla=cervejeira_dupla, cervejeira_bohemia=cervejeira_bohemia)
        #Roda o método que otimiza os refrigerantes
        otimizacao.otimiza_refrigerante(ponto=ponto, df_vendas=df_vendas, refrigerante_duplo=refrigerante_duplo)
        #Roda o método otimizar comum para as canaletas restantes
        otimizacao.otimiza(ponto=ponto, df_vendas=df_vendas)
        #Transforma os retornos num dataframe
        retornos = otimizacao.get_retornos(ponto)
        #Seleciona somente os retornos de Cerveja
        retornos_cerveja = retornos[retornos.Canaleta.str.contains("CV")]
        #Adiciona os retornos das cervejas à lista dos retornos
        lista_retornos.append(retornos_cerveja.Retorno.sum())
        #Salva os retornos numa tabela 
        retornos.to_excel("../retornos/" + str(ponto.local) + " Retornos.xlsx", index = False)
        #Roda o método que posta o planograma otimizado
        resposta_postar_planograma = input("Deseja postar o planograma? (S/N)")
        if resposta_postar_planograma == "S":
            otimizacao.postar_planograma(ponto)
            #Salva a otimização no histórico de otimizações
            hoje = pd.Timestamp(datetime.today().year, datetime.today().month, datetime.today().day)
            historico = pd.read_excel("../datasets/otimizacao/historico_otimizacoes.xlsx")
            historico = historico.append({"Dia": hoje, "Ponto": ponto.local}, ignore_index=True)
            historico.to_excel("../datasets/otimizacao/historico_otimizacoes.xlsx", index=False)
    except:
        print(f"Erro no ponto {ponto.local}")
        pontos_erro.append(ponto.local)

pdvs_retornos = pdvs[~pdvs.location_name.isin(pontos_erro)]
#%%
"""Célula responsável por otimizar os planogramas dos pontos de venda corporativos(MM)"""
pontos_erro = []
lista_retornos = []
for i in pdvs.location_name:
    #try:
    #Cria o ponto de venda(pdv)
    ponto = otimizacao.cria_ponto(i)
    otimizacao.get_planograma(ponto)
    #Roda o método otimizar comum para as canaletas restantes
    otimizacao.otimiza(ponto=ponto, df_vendas=df_vendas, espaco_otimizar={"CC":None,"PM":None,"GB":None,"FR":None, "RG":8}, abastecimento=3)
    #Transforma os retornos num dataframe
    retornos = otimizacao.get_retornos(ponto)
    #Salva os retornos numa tabela 
    retornos.to_excel("../retornos/" + str(ponto.local) + " Retornos.xlsx", index=False)
    #Roda o método que posta o planograma otimizado
    resposta_postar_planograma = input("Deseja postar o planograma? (S/N)")
    if resposta_postar_planograma == "S":
        otimizacao.postar_planograma(ponto)
        #Salva a otimização no histórico de otimizações
        hoje = pd.Timestamp(datetime.today().year, datetime.today().month, datetime.today().day)
        historico = pd.read_excel("../datasets/otimizacao/historico_otimizacoes.xlsx")
        historico = historico.append({"Dia": hoje, "Ponto": ponto.local}, ignore_index=True)
        historico.to_excel("../datasets/otimizacao/historico_otimizacoes.xlsx", index=False)
    #except:
    #    print(f"Erro no ponto {ponto.local}")
    #    pontos_erro.append(ponto.local)

pdvs_retornos = pdvs[~pdvs.location_name.isin(pontos_erro)]
# %% 
pdvs_retornos["retornos"] = lista_retornos
pdvs_retornos.sort_values(by = "retornos", ascending = False, inplace = True)
#%%
"""
Célula responsável por otimizar os planogramas dos pontos de venda selecionados
"""
lista_plan = []
pontos_erro = []
lista_retornos = []
lista_resultados = []
lista_fileiras = []
x = 0
for i in pdvs['location_name']:
#for i in df:
    #try:
    print(x)
    x += 1
    #Cria o ponto de venda(pdv)
    ponto = otimizacao.cria_ponto(i)
    #Roda o método Cervejeira Otimizada
    #otimizacao.otimiza_cervejeira(ponto = ponto, df_vendas = df_vendas, cervejeira_dupla = cervejeira_dupla, cervejeira_bohemia = cervejeira_bohemia)
    #lista_plan.append(ponto.planograma)
    #Roda o método que otimiza os refrigerantes
    #otimizacao.otimiza_refrigerante(ponto = ponto, df_vendas = df_vendas, refrigerante_duplo = refrigerante_duplo)
    #Roda o método otimizar comum para as canaletas restantes
    #otimizacao.otimiza(ponto = ponto, df_vendas = df_vendas)
    #Transforma os retornos num dataframe
    planograma = otimizacao.get_planograma(ponto)
    #otimizacao.otimiza(ponto = ponto, df_vendas = df_vendas, espaco_otimizar = {"PM":None,"GB":None,"FR":None})
    #lista_resultados.append(ponto.resultado)
    #lista_fileiras.append(ponto.fileiras)
    #retornos = otimizacao.get_retornos(ponto)
    #Seleciona somente os retornos de Cerveja
    #retornos_cerveja = retornos[retornos.Canaleta.str.contains("CV")]
    fileiras_ativas = planograma[(planograma.name.str.contains("CV")) & (planograma.status == "active")]
    lista_fileiras.append(fileiras_ativas.par_level.sum())
    #Adiciona os retornos das cervejas à lista dos retornos
    #lista_retornos.append(retornos_cerveja.Retorno.sum())
    #Salva os retornos numa tabela 
    #retornos.to_excel("./retornos_/" + str(ponto.local) + " Retornos.xlsx", index = False)
    #Roda o método que posta o planograma otimizado
    #import pdb;pdb.set_trace()
    #otimizacao.postar_planograma(ponto)
    #Salva a otimização no histórico de otimizações
    #hoje = datetime.today()
    #hoje = pd.Timestamp(hoje.year, hoje.month, hoje.day)
    #historico = pd.read_excel("./datasets/historico_otimizacoes.xlsx")
    #historico = historico.append({"Dia": hoje, "Ponto": ponto.local}, ignore_index = True)
    #historico.to_excel("./datasets/historico_otimizacoes.xlsx", index = False)
    #except:
    #    print("Erro")
    #    pontos_erro.append(i)

#%%

"""Célula responsável por otimizar os planogramas dos pontos de venda selecionados"""
pontos_erro = []
lista_retornos = []
for i in df_agrupado['local']:
    try:
        #Cria o ponto de venda(pdv)
        ponto = otimizacao.cria_ponto(i)
        #Roda o método Cervejeira Otimizada
        otimizacao.otimiza_cervejeira(ponto = ponto, df_vendas = df_vendas, cervejeira_dupla = cervejeira_dupla, cervejeira_bohemia = cervejeira_bohemia)
        #Transforma os retornos num dataframe
        retornos = otimizacao.get_retornos(ponto)
        #Seleciona somente os retornos de Cerveja
        retornos_cerveja = retornos[(retornos.Canaleta.str.contains("CV")) & (retornos.Recomendado == 0)]
        #Adiciona os retornos das cervejas à lista dos retornos
        lista_retornos.append(retornos_cerveja.Retorno.sum())
        #Salva os retornos numa tabela 
        retornos.to_excel("../retornos/" + str(ponto.local) + " Retornos.xlsx", index = False)
    except:
        print("Erro")
        pontos_erro.append(ponto.local)
# %%
def divide_fileiras(fileira, divisor_a, divisor_b):
    if fileira % divisor_a == 0:
        return fileira / divisor_a
    else:
        return fileira / divisor_b

def divide_fileiras_cv(fileira):
    if fileira % 6 == 0:
        return fileira / 6
    else:
        return fileira / 7