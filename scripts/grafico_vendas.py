'''
Arquivo para criar gráfico de vendas dos últimos N dias.

'''


import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objs as go
from datetime import MAXYEAR, datetime
from datetime import timedelta
from classe_planograma import importar_vendas

def gerar_grafico_vendas(n, from_api = False):
    vendas = importar_vendas('detached', only_mc = False, from_api = from_api)
    vendas = vendas[vendas['dia'] > datetime.strptime('2020-06-01', '%Y-%m-%d').date()]

    vendas_dias = vendas.groupby(['dia'])['valor'].sum().rolling(window=n).sum().dropna().reset_index()
    vendas_dias['contador'] = vendas_dias.index


    fim = max(vendas_dias['dia']) + timedelta(days = - 1)
    inicio = min(vendas_dias['dia'])
    lim_eixo_y = max(vendas_dias['valor'])
    dia_maxima = vendas_dias[vendas_dias['valor'] == lim_eixo_y]['dia'].iloc[0]
    dia_maxima = datetime.strftime(dia_maxima, '%d/%m/%Y')

    anotacao = 'Vendas acumuladas de ' + str(n) + ' dias em ' + datetime.strftime(fim, '%d/%m/%Y') + ': ' + "{:.2f}".format(float(vendas_dias.loc[vendas_dias['dia'] == fim,'valor']))
    anotacao2 = 'Máximo histórico: ' + "{:.2f}".format(lim_eixo_y) + ' em ' + dia_maxima
    print(anotacao)
    print(anotacao2)

    trace1 = go.Scatter(x = vendas_dias['dia'][:2],
    y = vendas_dias['valor'][:2],
    mode = 'lines',
    line=dict(width=1.5))

    frames = [dict(data = [dict(type = 'scatter',
                                x=vendas_dias['dia'][:k+1],
                                y=vendas_dias['valor'][:k+1]),
                        ],
                traces = [0],
                ) for k in range(1, len(vendas_dias))]

    layout = go.Layout(width = 1400,
                    height = 700,
                    showlegend=False,
                    hovermode = 'x unified',
                    updatemenus=[
                            dict(
                                type='buttons', showactive=True,
                                y=1.05,
                                x=1.15,
                                xanchor='right',
                                yanchor='top',
                                pad=dict(t=0, r=10),
                                buttons=[dict(label='Play',
                                method='animate',
                                args=[None, 
                                    dict(frame=dict(duration=3, 
                                                    redraw=False),
                                                    transition=dict(duration=0),
                                                    fromcurrent=True,
                                                    mode='immediate')]
                                )]
                            ),
                        ]              
                    )

    layout.update(xaxis =dict(range=[inicio, fim], autorange=False),
                yaxis =dict(range=[0, lim_eixo_y + 10000], autorange=False),
                title = 'Roda Conveniência - Evolução do Faturamento em ' + str(n) +' dias',
                margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="LightSteelBlue",)

    fig = go.Figure(data=[trace1], frames=frames, layout=layout)
    fig.add_annotation(xref = 'paper', yref = 'paper',
            x= 0, y= 1,
            text=anotacao,
            showarrow=False,
            align='left',
            font=dict(
            family="Open Sans",
            size=16))
    fig.add_annotation(xref = 'paper', yref = 'paper',
            x= 0, y= 0.95,
            text=anotacao2,
            showarrow=False,
            align='left',
            font=dict(
            family="Open Sans",
            size=16))
    return({'fig':fig,
            'data':datetime.strftime(fim, '%Y-%m-%d')})


n = 30
grafico = gerar_grafico_vendas(n = n, from_api = True)
grafico['fig'].write_html('./graficos-html/Roda Conveniencia - Vendas em ' + str(n) + ' dias.html')