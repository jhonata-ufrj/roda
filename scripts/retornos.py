"""
Módulo que trata os dados dos retornos
"""
import pandas as pd
banco_retornos = pd.read_excel("../datasets/retornos.xlsx")

def salva_retorno(retorno):
    retorno.to_excel(r"../datasets/retornos.xlsx", index = False)

def transforma_retorno(retorno):
    retorno = retorno[["Canaleta", "Produto", "Retorno"]]
    retorno = retorno[retorno["Canaleta"].str.contains("CV")]
    #retorno = retorno[retorno.Canaleta.isin(banco_retornos.Canaleta)]
    return retorno

def transforma_planograma(planograma):
    planograma.dropna(subset = ["par_level", "current_balance"], inplace = True)
    planograma["par_level"] = planograma["par_level"].apply(lambda x: x.replace(",", "."))
    planograma["current_balance"] = planograma["current_balance"].apply(lambda x: x.replace(",", "."))
    planograma = planograma[(planograma["name"].str.contains("CV")) & (planograma["status"] == "active") & (planograma["par_level"].astype(float) > planograma["current_balance"].astype(float))]
    planograma = planograma.rename(columns = {"name": "canaleta"})  
    planograma["envio"] = planograma["par_level"].astype(float) - planograma["current_balance"].astype(float) 
    planograma = planograma[["good_id", "canaleta", "envio"]]
    planograma = planograma[planograma.canaleta.isin(banco_retornos.Canaleta)]
    return planograma  

#Método que adiciona novos produtos ou quantidades à base de dados dos retornos
def atualiza_banco(retorno, linha, banco_retornos = banco_retornos):
    index = banco_retornos[banco_retornos.Canaleta == retorno.iloc[linha]["Canaleta"]].index[0]
    banco_retornos.at[index, "Retorno"] = retorno.iloc[linha]["Retorno"] + banco_retornos.iloc[index]["Retorno"]
    return banco_retornos

def retorno_cervejeira(retorno):
    return retorno[retorno["Canaleta"].str.contains("CV")]

#Método que varre o banco de dados dos retornos, recebe um planograma e retorna o que pode ser retirado dos retornos
def retorno_to_picklist(planograma, i):
    if planograma.iloc[i]["canaleta"] in banco_retornos.Canaleta.tolist():
        index_produto = banco_retornos[banco_retornos.Canaleta == planograma.iloc[i]["canaleta"]].index[0]
        produto = banco_retornos.iloc[index_produto]["Produto"]
        retorno = banco_retornos.iloc[index_produto]["Retorno"]
        qtd_planograma = int(planograma.iloc[i]["envio"])
        if retorno - qtd_planograma >= 0:
            sub = banco_retornos.iloc[index_produto]["Retorno"] - planograma.iloc[i]["envio"]
            banco_retornos.at[index_produto, "Retorno"] = int(sub)
            quantidade = qtd_planograma
        else:
            quantidade = banco_retornos.iloc[index_produto]["Retorno"]
            banco_retornos.at[index_produto, "Retorno"] = 0         
    return produto, quantidade, banco_retornos   



