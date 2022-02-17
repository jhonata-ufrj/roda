#%%

import plotly.express as px
import datetime
from classe_planograma import importar_vendas

#%%
vendas = importar_vendas('detached', dias = 30, only_mc=False)
# Cria coluna com a hora em formato numérico
vendas['hora_float'] = vendas['data'].dt.hour + vendas['data'].dt.minute / 60 + vendas['data'].dt.second / 3600
# %%
# Filtra os dias da análise para ultimos 28 dias (4 semanas)
vendas = vendas[vendas['dia'] > max(vendas['dia']) - datetime.timedelta(days = 28)]

# Define dia da semana para análise (0 é segunda e 6 é domingo)
dia_da_semana = 4

# Plota gráfico com o perfil horário de vendas
x = vendas[vendas['dia.semana'] == dia_da_semana].sort_values(by = 'hora_float')['hora_float']
y = vendas[vendas['dia.semana'] == dia_da_semana].sort_values(by = 'hora_float')['valor'].cumsum() / vendas[vendas['dia.semana'] == dia_da_semana].sort_values(by = 'hora_float')['valor'].sum()

fig  = px.line(x = x, y=y)
fig.show()
# %%
