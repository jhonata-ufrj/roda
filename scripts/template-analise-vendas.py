#%%
import pandas as pd
from classe_planograma import Ponto 

pontos_otimizados = pd.read_csv('./datasets/pontos-otimizados.csv')
pontos_otimizados['data'] = pd.to_datetime(pontos_otimizados['data']).dt.date

for i in pontos_otimizados.index:
    local = pontos_otimizados.loc[i,'local']
    dia_inicio = pontos_otimizados.loc[i,'data']
    ponto = Ponto(local)
    ponto.getPickList()
    ponto.calcularDesempenhoOtimizacao(dia_inicio=dia_inicio, dias = 30)