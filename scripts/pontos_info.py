#%%
import pandas as pd
import datetime
import vmconnection as vmpay

def importar_vendas(file_vendas_antigas = 'detached_1', only_mc = True, from_api = True, dias = None):
    '''
    Função que importa base de vendas. Pode importar uma base "antiga" já salva no pc local ou atualizar essa base antiga com vendas puxadas da api.
    
    Parâmetros
    ============
    file_vendas_antigas: string com o nome do arquivo de vendas antigas SEM extensão
    only_mc: Se verdadeiro, filtra a base de vendas para conter somente vendas de minimercados residenciais
    from_api: se verdadeiro, atualiza base de vendas com informações da API
    dias: número de dias. se não for none, base é filtrada para os últimos N dias.
    '''
    ## Carrega dataframe de vendas antigo
    vendas_antigo = pd.read_csv("./datasets/" + file_vendas_antigas + ".csv",sep = ";", low_memory = False)
    #df['occurred_at'] = pd.to_datetime(df['occurred_at'])

    if from_api:
        ## Puxa da API novas vendas
        venda_nova = vmpay.VMConnection().getPurchasesNewEndPoint()

    else:
        venda_nova = vendas_antigo
    ## Exclui dataframe com vendas antigas: ocupa muito espaço na memória
    del(vendas_antigo)

    # Filtra somente as linhas de pontos residenciais (MC) e que não são produto Teste
    if only_mc:
        venda_nova = venda_nova[(venda_nova['location_name'].str.find('MC |Mc ') != -1) & (venda_nova['good_name'] != 'Teste')]
    else:

        venda_nova = venda_nova[(venda_nova['good_name'] != 'Teste')]
    # seleciona somente algumas colunas

    # Selecionar somente colunas relevantes para nosso propósito

    colunas_vendas = ['request_number', # Requisição - identifica transações distintas
                    'machine_id', # Id da máquina - importante para merge com outras bases
                    'installation_id', # Id da instalação - importante para merge com outras bases
                    'eft_card_brand_name', # Nome do cartão - para análises de quais cartões são mais usados
                    'occurred_at', # Data e hora da transação
                    'location_name', # Nome do ponto de venda
                    'good_id', # Id do produto vendido
                    'good_name', # Nome do produto vendido
                    'transaction_value', # Valor da transação
                    'good_upc_code',
                    'good_barcode',
                    'coil',
                    'location_id',
                    'client_name'
                    ]
    venda_nova = venda_nova[colunas_vendas]

    # Renomear colunas para facilitar leitura

    venda_nova = venda_nova.rename(columns = {'request_number':'requisicao',
                                            'occurred_at':'data',
                                            'location_name':'local',
                                            'good_id':'id_produto',
                                            'good_name':'produto',
                                            'transaction_value':'valor',
                                            'eft_card_brand_name':'cartao'})

    # Faz alteração de tipo em colunas
    #import pdb;pdb.set_trace()
    venda_nova['data'] = pd.to_datetime(venda_nova['data']).dt.tz_convert('America/Sao_Paulo')
    venda_nova['machine_id'] = venda_nova['machine_id'].astype(str)
    venda_nova['installation_id'] = venda_nova['installation_id'].astype(str)
    venda_nova['id_produto'] = venda_nova['id_produto'].astype(str).str.replace(r'\.0','')

    # Cria colunas separadas com informações de dia, hora e dia da semana

    venda_nova['dia'] = venda_nova['data'].dt.date
    venda_nova['hora'] = venda_nova['data'].dt.time
    venda_nova['dia.semana'] = venda_nova['data'].dt.weekday

    if dias != None:
        venda_nova = venda_nova[venda_nova['dia'] > (max(venda_nova['dia']) - datetime.timedelta(days = dias))]

    return(venda_nova)
#%%
#lc = pd.DataFrame(vmpay.VMConnection().getLocations())
##MM = COMERCIAL
##MC = RESIDENCIAL 
#lista_tipos = []
#for nome in lc.name:
#    if ("CONDOMINIO" in nome) or ("MC" in nome) or ("Condomínio" in nome) or ("CONDOMÍNIO" in nome) or ("Condominio" in nome):
#        lista_tipos.append("residencial")
#    else:
#        lista_tipos.append("comercial")
#lc["tipo"] = lista_tipos
#lc.tipo.value_counts() 
#lc = lc[["id", "client_id", "name", "street", "number", "neighborhood", "city", "country", "state", "zip_code", "tipo"]]
#lc['endereco'] = lc["street"] + ', ' + lc["number"] + ', ' + lc["neighborhood"] + ', ' + lc["city"] + ', ' + lc["state"] + ', ' + lc["country"]
#lc.rename(columns = {"id": "location_id", "name": "location_name"}, inplace = True)
#lc = lc[["location_id", "client_id", "location_name", "tipo", "endereco", "zip_code"]]    
#lc = lc.T.drop_duplicates().T
df_vendas = importar_vendas(only_mc=False, from_api = True)
#df_vendas = df_vendas[["machine_id", "installation_id", "location_id", "client_name"]]
#df_vendas = df_vendas.drop_duplicates("location_id")
#df_dados = df_vendas.merge(lc, on = "location_id")
#df_dados.to_csv("locais.csv")
#print("Ok")

# %%
