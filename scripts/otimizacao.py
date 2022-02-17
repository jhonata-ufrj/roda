"""Módulo responsável por consumir os recursos da Classe Planograma que são mais voltados para a otimização"""

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

def cria_ponto(nome):
    """
    Função que instancia um objeto da classe ponto
    """
    ponto = Ponto(nome)
    return ponto

def get_retornos(ponto):
    retornos = ponto.retornos
    return retornos   

def postar_planograma(ponto):
    print(ponto.local)
    ponto.postarPlanograma()
    return 'Ok'    

def get_planograma(ponto, numero = -1):
    return ponto.getPlanograma(numero = numero)   

def salva_otimizacao(historico, hoje, local):
    return historico.append({"Dia":hoje, "Ponto":local}, ignore_index = True) 

def otimiza_cervejeira_natal(ponto, df_vendas, df_aux = pd.DataFrame(), df_cervejeira = pd.DataFrame(), limite_antigo = 0, pontos_errados = [], cervejeira_dupla = [], cervejeira_bohemia = [], abastecimento=2):
    print(ponto.local)
    if ponto.local in cervejeira_dupla: #Condicional para definir o limite de acordo com o espaço disponível na cervejeira
        espaco_cervejeira = 66
    elif ponto.local in cervejeira_bohemia:
        espaco_cervejeira = 40
    else:
        espaco_cervejeira = 34 
    ponto.getPlanograma()
    ponto.otimizar_alocadas(df=df_vendas, save=False, espaco={"CV":espaco_cervejeira}, var=['CV'], alocar=1, recomendar=1, abastecimento=abastecimento)
    ponto.analisarResultado()
    ponto.salvarPlanograma()
    return ponto.planograma

def otimiza_cervejeira(ponto, df_vendas, df_cervejeira=pd.DataFrame(), limite_antigo=0, pontos_errados=[], cervejeira_dupla=[], cervejeira_bohemia=[], abastecimento=2):
    print(ponto.local)
    if ponto.local in cervejeira_dupla:
        espaco_cervejeira = 66
        divisor = 4
    elif ponto.local in cervejeira_bohemia:
        espaco_cervejeira = 40
        divisor = 4
    else:
        espaco_cervejeira = 34
        divisor = 4

    chave = "cerveja"
    ponto.getPlanograma()
    ponto.otimizarDireto(df=df_vendas, save=False, espaco={"CV":espaco_cervejeira}, alocar=0, abastecimento=abastecimento)
    ponto.analisarResultado()
    demanda_cervejeira = ponto.fileiras.iloc[0]["Fileiras Recomendadas"]

    if demanda_cervejeira < espaco_cervejeira: #Condicional que verifica se há necessidade de aumentar o número de cervejeiras no pdv
        print(demanda_cervejeira, (espaco_cervejeira / divisor))
        #limite_antigo = 1000
        limite_antigo = espaco_cervejeira #Variável que recebe o antigo limite, que era o espaço disponível na cervejeira
        espaco_cervejeira = round(demanda_cervejeira + (espaco_cervejeira / divisor)) #Variável que representará o novo limite para rodar no algoritmo da cervejeira
    if espaco_cervejeira <= limite_antigo:  
        print(ponto.fileiras)
        ponto.otimizar_alocadas(df=df_vendas, save=False, espaco={"CV":espaco_cervejeira}, var=['CV'], alocar=1, recomendar =1, abastecimento=abastecimento)#Roda o algoritmo de alocar vazias alterando as fileiras recomendadas para '0')
    else:
        print(ponto.fileiras)
        ponto.otimizar_alocadas(df = df_vendas, save=False, espaco = {"CV":limite_antigo}, var = ['CV'], alocar = 1, recomendar =1) #Roda o algoritmo de alocar vazias alterando as fileiras recomendadas para '0'

    produtos = ponto.get_produtos()
    ponto.analisarResultado()
    planograma_atual = ponto.planograma[ponto.planograma.name.str.contains("CV")].reset_index(level = 0)
    cerva = ponto.retorno_balanco(planograma_atual, produtos, chave)
    ponto.retorno_abastecimento(cerva)

    return ponto.planograma
    
def otimiza_refrigerante(ponto, df_vendas, save=False, espaco_refrigerante=8, recomendado=1, refrigerante_duplo=[], abastecimento=2):
    print(ponto.local)

    if ponto.local in refrigerante_duplo:
        espaco_refrigerante = 16

    chave = "refrigerante"
    ponto.otimizar_alocadas(df=df_vendas, save=save, espaco={"RG": espaco_refrigerante}, recomendar=recomendado, alocar=1, abastecimento=abastecimento)
    ponto.analisarResultado()
    produtos = ponto.get_produtos()
    planograma_atual = ponto.planograma[ponto.planograma.name.str.contains("PG")].reset_index(level=0)
    refri = ponto.retorno_balanco(planograma_atual, produtos, chave)
    ponto.retorno_abastecimento(refri)

    return ponto.fileiras

def otimiza(ponto, df_vendas, save=False, espaco_otimizar={"CC":None,"PM":None,"GB":None,"FR":None} , recomendado=1, abastecimento=2):
    print(ponto.local)
    #chave = "comum"
    chave = "xxxxx"
    ponto.otimizarDireto(df=df_vendas, save=False, espaco=espaco_otimizar, abastecimento=abastecimento)
    ponto.analisarResultado()
    produtos = ponto.get_produtos()
    if "RG" in espaco_otimizar.keys():
        planograma_atual = ponto.planograma[(ponto.planograma.name.str.contains("CC")) | (ponto.planograma.name.str.contains("PM")) | 
        (ponto.planograma.name.str.contains("GB")) | (ponto.planograma.name.str.contains("FR")) | (ponto.planograma.name.str.contains("PG"))]
    else:    
        planograma_atual = ponto.planograma[(ponto.planograma.name.str.contains("CC")) | (ponto.planograma.name.str.contains("PM")) | 
        (ponto.planograma.name.str.contains("GB")) | (ponto.planograma.name.str.contains("FR"))]

    planograma_atual.reset_index(level = 0, inplace = True)
    comum = ponto.retorno_balanco(planograma_atual, produtos, chave)
    ponto.retorno_abastecimento(comum)

    return ponto.fileiras


