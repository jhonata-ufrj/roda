# Módulos do Python
import pandas as pd
import json
import numpy as np
import datetime
from IPython.display import display
# Arquivos .py, devem estar no diretório
import vmconnection as vmpay
import simular_vendas as simular

def importar_vendas(file_vendas_antigas = 'detached', only_mc = False, from_api = True, dias = 30):
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
    vendas_antigo = pd.read_csv("../datasets/vendas/" + file_vendas_antigas + ".csv",sep = ";", low_memory = False)
    #df['occurred_at'] = pd.to_datetime(df['occurred_at'])

    if from_api:
        ## Puxa da API novas vendas
        venda_nova = vmpay.VMConnection().getPurchases(vendas_antigo, save = True)

    else:
        venda_nova = vendas_antigo
    ## Exclui dataframe com vendas antigas: ocupa muito espaço na memória
    del(vendas_antigo)

    # Filtra somente as linhas de pontos residenciais (MC) e que não são produto Teste
    if only_mc:
        venda_nova = venda_nova[(venda_nova['location_name'].str.find('MC |Mc ') != -1) & (venda_nova['good_name'] != 'Teste')]
    else:
        venda_nova = venda_nova[(venda_nova['good_name'] != 'Teste')]

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
    venda_nova = venda_nova

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

class Ponto():
    '''
    Classe que representa um ponto de venda. Definido um ponto, é possível usar métodos para alterar seu planograma ou pegar informações sobre vendas daquele lugar em específico.

    '''

    def __init__(self,local):

        '''
        Cria uma instância da classe a partir de uma string com o pdv.
        
        Parâmetros
        ===========
        local: string. nome do pdv, exatamente como no sistema.

        Atributos
        ===========
        local: string. nome do pdv
        machine_id: inteiro. id da máquina
        installation_id: inteiro. id da instalação
        planograma: None ou Pandas DataFrame. Contém o planograma mais recente (ou um planograma carregado a partir de csv)
        prod_ign: lista. Contém ids dos produtos que não deverão ser considerados na otimização.
        '''

        locais = pd.DataFrame(vmpay.VMConnection().getLocations())
        maquinas = pd.json_normalize(vmpay.VMConnection().getMachines())
        locais = locais[['id','name']]
        locais.columns = ['location_id','location_name']
        maquinas = maquinas[['id','installation.location_id','installation.id']]
        maquinas.columns = ['machine_id','location_id','installation_id']
        locais = locais.merge(maquinas, on = ['location_id'], how='left')
        locais = locais.loc[locais['location_name'] == local].reset_index()
        locais = locais[~locais['machine_id'].isnull()]

        ### Atributos
        self.local = local
        try:
            self.machine_id = int(locais.loc[0,'machine_id'])
            self.installation_id = int(locais.loc[0,'installation_id'])
        except:
            self.machine_id = int(locais.loc[1,'machine_id'])
            self.installation_id = int(locais.loc[1,'installation_id'])
        self.planograma = None
        self.prod_ign = []
        self.locais = locais
        self.maquinas = maquinas
        self.retornos = pd.DataFrame()


    def getPlanograma(self, arquivo = None, numero = -1):

        '''
        Puxa o último planograma cadastrado na VMPay ou um planograma a partir de um arquivo em csv.
        
        Parâmetros:
        ==============
        arquivo: string. default = None. deve ser o caminho de um arquivo csv. Se estiver vazio (None), a função puxa o último planograma cadastrado.
        numero: inteiro (negativo) com a identificação do planograma para pegar da api. -1 é o último, -2 o penúltimo e assim por diante.

        Atributos alterados
        ==============
        planograma: Pandas Dataframe.
        max_logical_locator: valor do maior logical locator no planograma, usado em caso de adição de novos produtos.
        planograma_id: id do planograma, usado para atualizar e deletar planograma.
        '''
        # Puxa ultimo planograma
        if arquivo == None:
            planograma = vmpay.VMConnection().getPlanogram(self.machine_id,self.installation_id)[numero]
            planograma_itens = planograma['items']
            ## Cria uma nova lista com uma cópia do último planograma
            planogramaAtual = []
            for p in planograma_itens: 
                planogramaAtual.append(p)

            # Transforma planograma em DataFrame e seleciona as colunas que serão utilizadas
            df_planograma = pd.DataFrame(planogramaAtual)[['type','id','name','good_id','capacity','par_level','alert_level','desired_price','logical_locator','status','current_balance','children']]
            self.planograma_id = planograma['id']
        else:
            print("RODANDO ARQUIVO DO PC")
            df_planograma = pd.read_csv(arquivo,sep = ';',encoding='latin-1',decimal=',')

        df_planograma['name'] = df_planograma['name'].str.replace('\t','')
        # Transforma logical locator em número - essa parte só é usada quando um produto novo é adicionado (para podermos achar o máximo da coluna)
        df_planograma['logical_locator'] = pd.to_numeric(df_planograma['logical_locator'])
        # Define a coluna de good_id como índice do dataframe. Útil para atualizar o dataframe depois com os dados da otimização.
        df_planograma.set_index('good_id',inplace=True)
        self.planograma = df_planograma
        self.max_ll = max(self.planograma['logical_locator'].values)
        self.produtos_antes = np.nansum(self.planograma['par_level'])
        return self.planograma, self.produtos_antes, self.machine_id, self.installation_id, self.planograma_id

    def getPickList(self):
        '''
        DOCUMENTAR
        '''
        pickListsAux = []
        for p in vmpay.VMConnection().getPickListsId(self.machine_id,self.installation_id):
            aux = p
            pickListsAux.append(aux)
            #print('Obtendo ids das pick lists: ',index, '/',df_machines.shape[0])
        pickListsAux = pd.DataFrame(pickListsAux)

        # Pega produtos    
        prod = []
        for product in vmpay.VMConnection().getProducts():
            prod.append(product)
        prod = pd.DataFrame(prod)

        index = 0
        machine, installation, pickLists = [], [], []
        for index in range(pickListsAux.shape[0]):
            id, m, i = pickListsAux['id'][index],pickListsAux['machine_id'][index], pickListsAux['installation_id'][index]
            p = vmpay.VMConnection().getPickLists(m,i,id)
            machine.append(m)
            installation.append(i)
            pickLists.append(p)
            #print('Obtendo pick lists: ',index, '/',pickListsAux.shape[0])
        pickLists = pd.DataFrame(pickLists)
        pickLists['machine_id'] = machine
        pickLists['installation_id'] = installation
        item_id, planogram_item_id, quantity, good_id = [],[],[],[]
        pick_id, created_at,updated_at, planogram_id, group_id, distribution_center_id, lugar = [],[],[],[],[],[],[]
        machine.clear()
        installation.clear()
        for index in range(pickLists.shape[0]):
        #for index in range(5):
            for i in pickLists['items'][index]:
                i = str(i)
                i = i.split('},')
                #print(i)
                for item in i:
                    item = item.split(", '")
                    
                    idAux = item[0]
                    idAux = idAux.split(':' )[1]

                    planogram_item_idAux = item[1]
                    planogram_item_idAux = planogram_item_idAux.split(':' )[1]

                    quantityAux = item[2]
                    quantityAux = quantityAux.split(':' )[1]

                    good_idAux = item[4]
                    good_idAux = good_idAux.split(':' )[1]
                    good_idAux = good_idAux.split('}' )[0]

                    pick_id.append(pickLists['id'][index])
                    created_at.append(pickLists['created_at'][index])
                    updated_at.append(pickLists['updated_at'][index]) 
                    planogram_id.append(pickLists['planogram_id'][index]) 
                    group_id.append(pickLists['group_id'][index]) 
                    distribution_center_id.append(pickLists['distribution_center_id'][index]) 
                    machine.append(pickLists['machine_id'][index])
                    installation.append(pickLists['installation_id'][index])

                    item_id.append(idAux)
                    planogram_item_id.append(planogram_item_idAux )
                    quantity.append(quantityAux)
                    good_id.append(good_idAux)   

        ##################
        pickListFinal = pd.DataFrame()
        pickListFinal['place'] = lugar 
        pickListFinal['machine_id'] = machine
        pickListFinal['installation_id'] = installation
        pickListFinal['pick_id'] = pick_id
        pickListFinal['created_at'] = created_at
        pickListFinal['updated_at'] = updated_at
        pickListFinal['planogram_id'] = planogram_id
        pickListFinal['group_id'] = group_id
        pickListFinal['distribution_center_id'] = distribution_center_id
        pickListFinal['item_id'] = item_id
        pickListFinal['planogram_item_id'] = planogram_item_id
        pickListFinal['quantity'] = quantity
        pickListFinal['good_id'] = good_id

        pickListFinal['product_name'] = ""
        for index in range(pickListFinal.shape[0]):
            for index2 in range(prod.shape[0]):
                item_pick = pickListFinal['good_id'][index]
                item_pick = int(item_pick)
                item_prod = prod['id'][index2]
                if item_pick == item_prod:
                    pickListFinal['product_name'][index] = prod['name'][index2]


        # Renomeia coluna de produtos
        pick_lists = pickListFinal.rename(columns = {'good_id':'id_produto',
                                            'product_name':'produto'})

        # Faz alteração de tipo em colunas
        pick_lists['created_at'] = pd.to_datetime(pick_lists['created_at']).dt.tz_localize(None) # Tira o fuso horário da data, que pode atrapalhar em manipulações da data
        pick_lists['machine_id'] = pick_lists['machine_id'].astype(str)
        pick_lists['id_produto'] = pick_lists['id_produto'].astype(str)

        # Renomeia coluna de planogram_id
        pick_lists = pick_lists.rename(columns = {'planogram_id':'planograma_id'})

        # Cria colunas de hora
        pick_lists['hora'] = pick_lists['created_at'].dt.hour

        # Cria coluna indicando o dia que devemos considerar para o abastecimento: se a hora da pick list é maior que 22h, assume que o reabastecimento foi feito no dia seguinte
        pick_lists['dia_correto'] = [(pick_lists['created_at'][i] + datetime.timedelta(days = 1)).date() if i >= 22 else pick_lists['created_at'][i].date() for i in pick_lists.index]

        # Filtra pick_lists para pegar dados somente após o início da operação de madrugada (01/12/2020)
        # Não faz essa parte: precisamos pegar os abastecimentos desde o início da operação para sabermos o estoque.
        #pick_lists = pick_lists[pick_lists['created_at'] > datetime.datetime.fromisoformat('2020-12-01')]

        # Filtra somente os valores em que local =/= nan (pick lists dos pontos residenciais)
        #pick_lists = pick_lists[pick_lists['place'].notna()]

        self.picklist = pick_lists

    def getAjustes(self):
        '''
        Importa base de ajustes e indica se o ajuste foi devido a furto ou perda / outros.


        '''


        plan = []
        for plano in vmpay.VMConnection().getInventoryAdjustments(machine = str(self.machine_id),inst = str(self.installation_id)):
            plan.append(plano)

        plan = pd.json_normalize(plan)

        ajustes = []
        for i in range(len(plan)):
            temp = plan.iloc[i]
            if (temp['kind'] == 'now') | (temp['kind'] == 'checkpoint'):
                tipo = temp['kind']
                date = temp['occurred_at']
                planogram_id = temp['planogram_id']
                items = temp['items']
                pl = vmpay.VMConnection().getSpecificPlanogram(self.machine_id,self.installation_id,planogram_id)
                plinfo =[]
                for a in range(len(pl['items'])):
                    plid = pl['items'][a]['id']
                    prodname = pl['items'][a]['good']['name']
                    prodid = pl['items'][a]['good']['id']
                    prodprice = pl['items'][a]['desired_price']
                    plinfo.append({"Id no Planograma":plid,"Nome":prodname,"Id Produto":prodid,"Preço":prodprice})
                
                for j in range(len(items)):
                    price = None
                    planoID = items[j]['planogram_item']['id'] 
                    antes = items[j]['balance_before']
                    depois = items[j]['balance_after']
                    nome = items[j]['planogram_item']['good']['name']
                    id_produto = items[j]['planogram_item']['good']['id']
                    for b in range(len(plinfo)):
                        if planoID == plinfo[b]["Id no Planograma"]:
                            price = pl['items'][b]['desired_price']

                    ajustes.append({"machine_id":self.machine_id,"data":date,"planograma":planogram_id,"tipo":tipo,"id_produto":id_produto,"produto":nome,"antes":antes,"depois":depois,"ajuste":antes-depois,"preço":price})
        
        ajustes = pd.DataFrame.from_dict(ajustes)

        if len(ajustes) > 0:
            # Alterar tipo da coluna de máquina para permitir merge com maquinas
            ajustes['machine_id'] = ajustes['machine_id'].astype(str)
            ajustes['id_produto'] = ajustes['id_produto'].astype(str)

            # Alterar tipos dos ajustes de para furto e perda
            tipo_furto = ['now','checkpoint']
            ajustes['tipo'] = ['furto' if ((ajustes['tipo'][j] in tipo_furto) & (ajustes['ajuste'][j] > 0)) else 'perda' for j in ajustes.index]

            ## Criar coluna com o dia:
            ajustes['data'] = pd.to_datetime(ajustes['data'])
            ajustes['dia'] = [ajustes['data'][i].date() for i in ajustes.index]


        self.ajustes = ajustes

    def calcularRuptura(self, dias):
        '''
        DOCUMENTAR
        '''
        # Importar bases utilizadas. Já filtro a base de vendas para conter só os dados do pdv.

        planograms = self.picklist['planograma_id'].unique()
        planogramas_lista = []
        for x in planograms:
            planograma_temp = pd.json_normalize(vmpay.VMConnection().getSpecificPlanogram(self.machine_id,self.installation_id,x))
            planogramas_lista.append(planograma_temp)

        plan = pd.concat(planogramas_lista).reset_index()

        planogramas = []
        for x in plan.index:
            teste = pd.json_normalize(plan.loc[x, 'items'])
            planogramas.append(teste)

        reabastecimento = self.picklist[~self.picklist.produto.str.contains('Sorvete|Picolé')][['planograma_id','id_produto','pick_id','dia_correto','quantity']]
        # Transforma tipos de colunas para inteiro
        reabastecimento['id_produto'] = reabastecimento['id_produto'].astype(int)
        reabastecimento['planograma_id'] = reabastecimento['planograma_id'].astype(int)
        # Renomeia colunas
        reabastecimento.columns = ['planogram_id','good_id','pick_id','dia','quantity']
        # Transforma NA em 0
        reabastecimento.fillna(0, inplace = True)
        # Junto os produtos de cada planograma
        plan = pd.concat(planogramas)[['planogram_id','good_id','par_level']]
        # Transforma tipos de colunas para inteiro
        plan['planogram_id'] = plan['planogram_id'].astype(int)
        plan['good_id'] = plan['good_id'].astype(int)
        plan.fillna(0, inplace=True)
        # Junta bases de pick lists (reabastecimento) com planogramas
        quantidades = reabastecimento.merge(plan, on = ['planogram_id','good_id'], how = 'outer')
        quantidades.fillna(0,inplace=True)
        # Transforma tipos de colunas
        quantidades['par_level'] = quantidades['par_level'].astype(int)
        quantidades['quantity'] = quantidades['quantity'].astype(int)
        # Calcula quantidade de produtos no pdv
        quantidades['qtd_local'] = quantidades['par_level'] - quantidades['quantity']
        # Filtro entradas relevantes
        quantidades = quantidades[quantidades['pick_id']>0]

        # Cria dataframe em que cada linha é um dia e cada coluna é um produto, com a quantidade de itens por produto no PDV
        quantidades_local = (quantidades.groupby(['dia','good_id'])['par_level'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'par_level').fillna(method = 'ffill') - quantidades.groupby(['dia','good_id'])['quantity'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'quantity').fillna(0)).reset_index(drop=True)

        # Trato dataframe para que produtos novos não apareceram como ruptura (quantidade = 0)
        for i in quantidades_local:
            quantidades_local[i] = [np.nan if ((quantidades_local.loc[x,i] == 0) and (x > 0) and (np.isnan(quantidades_local.loc[x-1,i]))) else quantidades_local.loc[x,i] for x in quantidades_local.index]

        # Calcuma quantidade de produtos que estão com quantidade no PDV = 0
        rupturas_pct = (quantidades_local == 0).sum(axis = 1) / (quantidades_local >= 0).sum(axis = 1)

        # Cria coluna com os dias das pick lists
        rupturas_pct.index = quantidades.groupby(['dia','good_id'])['par_level'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'par_level').index
        rupturas_pct = rupturas_pct.reset_index()
        rupturas_pct['dia'] = pd.to_datetime(rupturas_pct['dia']).dt.date

        # Filtra dataframe para os últimos N dias (parametro da função)
        rupturas_pct = rupturas_pct[(rupturas_pct['dia'] > (max(rupturas_pct['dia']) - datetime.timedelta(days = dias)))]

        # Coloca a coluna de dia como índice
        rupturas_pct.set_index('dia', inplace=True)
        
        # Calcula a média dos percentuais de rupturas por dia de pick list
        percent_rupturas = (np.mean(rupturas_pct) * 100)[0]
        self.rup_pct = percent_rupturas

    def calcularDesempenhoOtimizacao(self, dia_inicio, dias):

        # Importar bases utilizadas. Já filtro a base de vendas para conter só os dados do pdv.
        planograms = self.picklist['planograma_id'].unique()
        planogramas_lista = []
        for x in planograms:
            planograma_temp = pd.json_normalize(vmpay.VMConnection().getSpecificPlanogram(self.machine_id,self.installation_id,x))
            planogramas_lista.append(planograma_temp)

        plan = pd.concat(planogramas_lista).reset_index()

        planogramas = []
        for x in plan.index:
            teste = pd.json_normalize(plan.loc[x, 'items'])
            planogramas.append(teste)

        reabastecimento = self.picklist[~self.picklist.produto.str.contains('Sorvete|Picolé')][['planograma_id','id_produto','pick_id','dia_correto','quantity']]
        reabastecimento['id_produto'] = reabastecimento['id_produto'].astype(int)
        reabastecimento['planograma_id'] = reabastecimento['planograma_id'].astype(int)
        reabastecimento.columns = ['planogram_id','good_id','pick_id','dia','quantity']
        reabastecimento.fillna(0, inplace = True)
        plan = pd.concat(planogramas)[['planogram_id','good_id','par_level']]
        plan['planogram_id'] = plan['planogram_id'].astype(int)
        plan['good_id'] = plan['good_id'].astype(int)
        plan.fillna(0, inplace=True)
        quantidades = reabastecimento.merge(plan, on = ['planogram_id','good_id'], how = 'outer')
        quantidades.fillna(0,inplace=True)
        quantidades['par_level'] = quantidades['par_level'].astype(int)
        quantidades['quantity'] = quantidades['quantity'].astype(int)
        quantidades['qtd_local'] = quantidades['par_level'] - quantidades['quantity']
        quantidades = quantidades[quantidades['pick_id']>0]

        quantidades.groupby(['dia','good_id'])['par_level'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'par_level').fillna(method = 'ffill')

        quantidades_local = (quantidades.groupby(['dia','good_id'])['par_level'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'par_level').fillna(method = 'ffill') - quantidades.groupby(['dia','good_id'])['quantity'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'quantity').fillna(0)).reset_index(drop=True)

        for i in quantidades_local:
            quantidades_local[i] = [np.nan if ((quantidades_local.loc[x,i] == 0) and (x > 0) and (np.isnan(quantidades_local.loc[x-1,i]))) else quantidades_local.loc[x,i] for x in quantidades_local.index]

        rupturas_pct = (quantidades_local == 0).sum(axis = 1) / (quantidades_local >= 0).sum(axis = 1)
        rupturas_pct.index = quantidades.groupby(['dia','good_id'])['par_level'].sum().reset_index() \
            .pivot_table(index = 'dia',columns = 'good_id',values = 'par_level').index
        rupturas_pct = rupturas_pct.reset_index()
        rupturas_pct['dia'] = pd.to_datetime(rupturas_pct['dia']).dt.date

        rupturas_pct = rupturas_pct[(rupturas_pct['dia'] > (dia_inicio - datetime.timedelta(days = dias))) & (rupturas_pct['dia'] < (dia_inicio + datetime.timedelta(days = dias)))]

        rupturas_pct.set_index('dia', inplace=True)

        ## PEGA MÉDIA LOGO ANTES DA INTERVENÇÃO, MÉDIA DEPOIS E COMPARA.
        intervencao = dia_inicio
        daterange = pd.date_range(start = min(rupturas_pct.index), end = intervencao + datetime.timedelta(days = dias))
        calendario = pd.DataFrame({0:np.nan}, index = daterange)
        calendario.update(rupturas_pct)

        percent_rupturas_antes = (np.mean(calendario.loc[:intervencao,]) * 100)[0]
        percent_rupturas_depois = (np.mean(calendario.loc[intervencao:,]) * 100)[0]

        variacao_ruptura = (percent_rupturas_depois - percent_rupturas_antes) / percent_rupturas_antes
        print(f'Resultado da otimização em {self.local} - Análise com {dias} dias.')
        print(f'Percentual de rupturas antes da intervenção: {percent_rupturas_antes}')
        print(f'Percentual de rupturas após a intervenção: {percent_rupturas_depois}')
        print(f'Variação: {variacao_ruptura}')
        self.var_rup = variacao_ruptura

    def setEspacos(self,espaco=None, arquivo_espacos = 'espacos.json'):

        '''
        Define o dicionário de espaços que será usado na função de otimização. Se espaco = None, procura no json 'espacos', e se não achar, usa o dicionario default.
        
        Parâmetros
        ============
        espaco: Dicionário. Deve conter o dicionário no formato {'Categoria com 2 digítos maiúsculos':Número de fileiras}.
        arquivo_espacos: Nome do arquivo com espaços que é consultado se espaco = None.

        Atributos alterados
        ============
        espacos. Dicionário com espaços que será usado na função de otimização.

        '''
        if espaco == None:
            with open('../datasets/' + arquivo_espacos) as json_file:
                espacos = json.load(json_file)
            try:
                espacos = espacos[str(self.machine_id)]
                # muda -1 no dicionario de espaços para 'None':
                for key, value in espacos.items():
                    if value == -1:
                        espacos[key] = None
            except KeyError:
                espacos = {'CV': 30, 'RG': 8, 'CC': None, 'PM': None, 'GB': None, 'FR': None}
                #espacos = {'CV': 35}
            self.espacos = espacos
        else:
            self.espacos = espaco
        return self.espacos    

    def getLimites(self, limites_file = '../datasets/otimizacao/limite_prateleira.csv'):

        '''
        Importar csv com validades e limites por prateleira. Informações usadas na otimização

        Parâmetros
        ============
        limites_file. string com o caminho do arquivo csv

        Atributos Alterados
        ============
        limites. dataframe com informações sobre limites por prateleira, validade e produto essencial para cada produto.

        '''
        
        limites_prateleira = pd.read_csv(limites_file,sep = ';', decimal = ',')
        limites_prateleira['id_produto'] = limites_prateleira['id_produto'].astype(str).str.replace(r'\.0','')
        limites_prateleira['limite_prateleira'] = limites_prateleira['limite_prateleira'].fillna(value=3)
        limites_prateleira['limite_prateleira'] = limites_prateleira['limite_prateleira'].astype(float)
        limites_prateleira['peso'] = limites_prateleira['peso'].fillna(value=1)
        limites_prateleira['peso'] = limites_prateleira['peso'].astype(int)
        limites_prateleira['data_inclusao'] = pd.to_datetime(limites_prateleira['data_inclusao'], format = "%d/%m/%Y", errors = "coerce")
        self.limites = limites_prateleira

    def getDadosVendas(self, dias):

            '''
            Pega dados de vendas no formato com média e desvio padrão
            
            Parâmetros
            ===========
            df_vendas. dataframe de vendas
            dias. inteiro com numero de dias considerados na analise
            
            Atributos Alterados
            ===========
            dados_vendas. dataframe com dados de vendas (media e desvio) por produto
            '''

            # Se o parâmetro dias não for vazio, filtro a base de vendas para ter os últimos N dias.
            if dias != None:
                vendas = self.vendas[self.vendas['dia'] > (max(self.vendas['dia']) - datetime.timedelta(days=dias))]
            
            # Crio um dataframe com todos os dias do período analisado. Isso é importante porque ao fazer groupby nas vendas de produtos por dia, podemos acabar sem algum dia (venda 0), o que puxaraia a média para cima.
            calendario = pd.DataFrame(index = pd.date_range(start = min(vendas['dia']), end = max(vendas['dia']))).reset_index().rename(columns = {'index':'dia'})
            calendario['dia'] = calendario['dia'].dt.date

            # Agrupo dados de vendas por id_produto e dia e conto a quantidade de produtos vendidos.
            # Transformo essa base agrupada para conter um produto por coluna e um dia por linha.
            self.consumo = vendas.groupby(['id_produto','dia'])['valor'].count().reset_index() \
                .pivot_table(index = 'dia',columns = 'id_produto', values = 'valor').reset_index()
            # Junto os dataframes de vendas e de calendario (que contém todos os dias do período) preenchendo os valores 'na' com 0
            self.consumo = calendario.merge(self.consumo, how = 'left', on ='dia').fillna(0)
            # Retorno o dataframe para o formato em que uma linha é um dia de um produto (agora temos linhas com vendas = 0)
            self.consumo = self.consumo.melt(id_vars = 'dia', var_name = 'id_produto', value_name='valor')
            # Junta base de vendas com limite, mantendo somente os produtos incluídos nas duas bases
            self.consumo = self.consumo.merge(self.limites, how = 'right',on = 'id_produto')
            # Altera a coluna 'produto_essencial' para 1 se a data de inclusão for há menos de 1 mês
            self.consumo['produto_essencial'] = [1 if self.consumo.loc[i,'data_inclusao'] > (datetime.datetime.today().date() - datetime.timedelta(days=dias)) else self.consumo.loc[i,'produto_essencial'] for i in self.consumo.index]
            self.consumo = self.consumo.fillna(0)
            # Agrupo a base tratada por produto e pego a média e o desvio padrão
            self.consumo = self.consumo.groupby(['id_produto','produto_essencial','limite_prateleira','validade_baixa','peso'])['valor'].agg(['mean','std']).reset_index().fillna(0)
            # Ajusto o desvio padrão: máximo entre média de vendas * 0.6 e o desvio real 
            self.consumo['std'] = np.minimum(np.array(self.consumo['std']), np.array(self.consumo['mean'] * 0.6))
            
            # Transforma id_produto em inteiro e passa para o index para poder fazer 'merge' com o planograma
            self.consumo['id_produto'] = self.consumo['id_produto'].astype(int)
            self.consumo.set_index('id_produto', inplace = True)
            # Junta com dataframe do planograma, mantendo somente produtos incluídos nas duas bases
            self.consumo = self.consumo.join(self.planograma[['name','par_level','status','current_balance']], how='right').fillna(0)
            # Renomeia colunas e reseta o index para voltar id_produto para uma coluna
            self.consumo = self.consumo.rename(columns = {'mean':'venda_media','std':'venda_desvio','par_level':'nivel_par'})
            self.consumo = self.consumo.reset_index(drop=False)
            try:
                self.consumo['id_produto'] = self.consumo['good_id'].astype(str)
                self.consumo.drop('good_id',axis = 1,inplace=True)
            except:
                self.consumo['id_produto'] = self.consumo['index'].astype(str)
                self.consumo.drop('index',axis = 1,inplace=True)

            # Junta base tratada com a base original de vendas para pegar o nome do produto.
            self.consumo = self.consumo.merge(self.vendas[['id_produto','produto']], how = 'left', on ='id_produto')

            # Cria coluna de canaleta com os 2 últimos digítos da segunda parte de 'name'
            self.consumo['canaleta'] = self.consumo['name'].str.split(pat = '-',expand = True)[1].str[-2:]
            # Troca canaletas: PP vira PM e produtos com 'PET' no nome viram RG (refri grande)
            self.consumo['canaleta'] = self.consumo['canaleta'].str.replace('PP','PM')
            self.consumo.loc[self.consumo['name'].str.split(pat = '-',expand = True)[2].str[-2:] == 'PG', 'canaleta'] = 'RG'
            self.consumo = self.consumo[self.consumo['limite_prateleira']>0]

            # Remove entradas duplicadas.
            self.consumo.drop_duplicates(subset = ['id_produto'], inplace = True)
        

    def getProdutosMaisVendidos(self, dias, n_produtos, vendas):

        '''
        Método para criar dataframe com os M produtos mais vendidos em um PDV nos últimos N dias.

        Parâmetros
        ===========
        vendas: dataframe de vendas (output de importar_vendas)
        dias: inteiro com o número de dias que se quer a análise
        n_produtos: número de produtos que deseja no dataframe final.

        Atributos Alterados
        ===========
        mais_vendidos: dataframe com a relação de produtos mais vendidos (por faturamento)
        '''
        return vendas.loc[(vendas['local'] == self.local) & (vendas['dia'] > max(vendas['dia']) - datetime.timedelta(days = dias))].groupby('produto')['valor'].sum() \
            .reset_index().sort_values(by='valor',ascending=False).head(n_produtos)

        print(self.mais_vend)

    def get_vendas_ponto(self, nome_ponto, df_vendas = None):
        if type(df_vendas) == type(None):
            print("passei por aq bb")
            df_vendas = importar_vendas(only_mc = False)
            return df_vendas[df_vendas['local'] == self.local]
        else:
            return df_vendas[df_vendas['local'] == self.local]
    
    def getVendas(self, df = None):
        '''
        DOCUMENTAR
        '''
        if type(df) == type(None):
            self.vendas = importar_vendas(only_mc = False)
            self.vendas = self.vendas[self.vendas['local'] == self.local]
        else:
            self.vendas = df[df['local'] == self.local]

    def otimizar(self, save=True, tipo_simulacao='genextreme', lista_alocar_vazias=['RG'], alocar=0, recomendar=0, abastecimento=2):

        '''
        Otimiza o planograma conforme dicionário de espaços.
        
        Parâmetros
        ===========
        df_vendas: Pandas DataFrame. Dataframe output da função importar_vendas do módulo importar_datasets.
        abastecimento: Tempo entre um abastecimento e outro. É usado para calcular o máximo de demanda (sempre no número de dias entre abastecimentos)

        Atributos alterados
        ===========
        planograma: Pandas Dataframe. Alterado com as quantidades novas e com status 'suspended' para produtos sem vendas nos últimos 30 dias.

        TODO incluir parâmetro 
        '''

        tempo = datetime.datetime.strftime(datetime.datetime.today(), format = '%Y-%m-%d %H-%M-%S')

        # Inclui no atributo 'produtos desconsiderar' os produtos que estão suspensos no planograma atual.

        self.prod_ign.extend(self.consumo[self.consumo['status'] == 'suspended']['id_produto'])
        self.prod_ign = list(set(self.prod_ign))

        # Inicializando listas que vão conter o resultado final:
        id_produtos = []
        produtos = []
        nivel_par_recomendado = []
        nivel_par_atual = []
        numero_fileiras = []
        demanda_maxima_simulada = []
        medias_consumo_df = []
        desvios_consumo_df = []
        fileiras_ocupadas_df = []
        pesos_df = []
        lista_fileiras_recomendadas = []
        lista_fileiras_ocupadas = []
        lista_fileiras_real = []
        lista_equipamentos = []
        # import pdb; pdb.set_trace()

        # Para cada espaço que vamos otimizar:
        for key, value in self.espacos.items():
            print(f'Otimizando {key}')
            # Filtro o Dataframe de métricas para incluir somente os produtos da categoria:
            metricas_estoque_categoria = self.consumo[self.consumo['canaleta'] == key]

            # Calcular número de fileiras já ocupadas por produtos suspensos, para tirar do limite_espaço
            #if value != None:
            #    fileiras_prod_susp = metricas_estoque_categoria[metricas_estoque_categoria['id_produto'].isin(self.prod_ign)][['produto','current_balance','limite_prateleira']]
            #    fileiras_prod_susp['current_balance'] = [fileiras_prod_susp.loc[i,'current_balance'] if fileiras_prod_susp.loc[i,'current_balance'] >= 0 else 0 for i in fileiras_prod_susp.index]
            #    #print(fileiras_prod_susp)
            #    fileiras_prod_susp = np.round(np.sum(fileiras_prod_susp['current_balance'] / fileiras_prod_susp['limite_prateleira']),0)
                #print(f'{fileiras_prod_susp} já ocupadas por produtos suspensos. Otimizando somente {value - fileiras_prod_susp}')
            #    limite_espaco = value - fileiras_prod_susp # De acordo com o dicionário que definimos
            #else:
            limite_espaco = value    



            # Filtra Dataframe para tirar produtos suspensos:
            metricas_estoque_categoria = metricas_estoque_categoria[~metricas_estoque_categoria['id_produto'].isin(self.prod_ign)]
            # Calcula o número de fileiras que os produtos estão ocupando atualmente.
            fileiras_peso_atuais = sum(metricas_estoque_categoria['nivel_par'] / metricas_estoque_categoria['limite_prateleira'] * metricas_estoque_categoria['peso'])
            print(f'Número de fileiras-peso planograma atual: {fileiras_peso_atuais}')
            fileiras_ocupadas = list((metricas_estoque_categoria['current_balance'] / metricas_estoque_categoria['limite_prateleira']) * metricas_estoque_categoria['peso'])
            # IMPRIMO FILEIRAS OCUPADAS POR PRODUTO
            #for produto, ocupacao in zip(metricas_estoque_categoria['produto'], fileiras_ocupadas):
            #    print(produto, ocupacao)
            # Retiro do Dataframe as informações que usaremos na função de otimizar_capacidade, colocando elas em formato de lista.    
            medias_consumo = list(metricas_estoque_categoria.venda_media)
            desvios_consumo = list(metricas_estoque_categoria.venda_desvio)
            limites_prateleira = list(metricas_estoque_categoria.limite_prateleira)
            validade_baixa = list(metricas_estoque_categoria.validade_baixa)
            produtos_essenciais = list(metricas_estoque_categoria.produto_essencial)
            pesos = list(metricas_estoque_categoria.peso)
            # Defino os demais parâmetros da função otimizar_nivel_par_simples:
            dias_simular = 30 #Número de dias de demanda para simular
            alocar_vazias = [1 if key in lista_alocar_vazias else 0][0]
            
            if alocar == 1:
                alocar_vazias = True
            else:
                alocar_vazias = False
                

            n_simulacoes = 1000 # Número de vezes que a função simula os N dias de demanda para calcular a demanda máxima
            # Chamo a função de otimização, que retorna um dicionário com algumas listas.
            print(f'{sum(fileiras_ocupadas)} fileiras já ocupadas e limite de espaço é {limite_espaco}')
            
            if recomendar == 1:
                soma_fileiras = 0
            else:
                soma_fileiras = sum(fileiras_ocupadas)    

            #print("Soma: {}".format(sum(fileiras_ocupadas)))
            # import pdb;pdb.set_trace()
            if limite_espaco == None or limite_espaco > soma_fileiras:
                nivel_par_categoria = simular.otimizar_nivel_par_simples(medias_consumo, desvios_consumo, dias_simular, abastecimento, limites_prateleira, fileiras_ocupadas, validade_baixa, produtos_essenciais, pesos, limite_espaco, n_simulacoes, tipo = tipo_simulacao, alocar_vazias=alocar_vazias,recomendar=recomendar)
                # Incluo os resultados nas listas finais.
                produtos.extend(list(metricas_estoque_categoria['produto']))
                id_produtos.extend(list(metricas_estoque_categoria['id_produto']))
                nivel_par_atual.extend(list(metricas_estoque_categoria['nivel_par']))
                nivel_par_recomendado.extend(nivel_par_categoria['niveis_par'])  
                numero_fileiras.extend(nivel_par_categoria['num_fileiras'])
                demanda_maxima_simulada.extend(nivel_par_categoria['max_demanda_simulada'])
                medias_consumo_df.extend(medias_consumo)
                desvios_consumo_df.extend(desvios_consumo)
                fileiras_ocupadas_df.extend(fileiras_ocupadas)
                pesos_df.extend(pesos)
                lista_fileiras_ocupadas.append(sum(fileiras_ocupadas))
                lista_fileiras_recomendadas.append(sum(nivel_par_categoria['num_fileiras']))
                lista_fileiras_real.append(sum(np.maximum(fileiras_ocupadas, nivel_par_categoria['num_fileiras'])))
                lista_equipamentos.append(key)
        # Rodados todos os espaços, precisamos somente retornar os dados de forma limpa.
        # Cria um dataframe com os dados:
        planograma_df = pd.DataFrame({'id_produto':id_produtos,
                                        'produto':produtos,
                                        'nivel_par_atual':nivel_par_atual,
                                        'nivel_par_recomendado':nivel_par_recomendado,
                                        'numero_fileiras':numero_fileiras,
                                        'demanda_maxima_simulada':demanda_maxima_simulada,
                                        'media_demanda':medias_consumo_df,
                                        'desvio_demanda':desvios_consumo_df,
                                        'fileiras_ocupadas':fileiras_ocupadas_df,
                                        'peso':pesos_df})
            # Se o parametro save = True, salvamos o dataframe em um csv
        if save:
            planograma_df.to_csv('../datasets/planogramas/Planograma Otimizado - ' + self.local + " - " + str(tempo) + '.csv', sep = ';', decimal = ',', encoding = 'latin-1', index = False)
            # Retorna um dicionario com 4 chaves: o Dataframe, uma lista já pronta para inclusão na função de novoPlanograma, e as ids de machine e instalação
        self.res_otim = planograma_df

        # Separa: produtos com par_level maior que 0 vão ter quantidade alterada 
        produtos_alterar = planograma_df[planograma_df['nivel_par_recomendado'] > 0][['id_produto','nivel_par_recomendado']]
        # Renomeia colunas para ficarem com mesmo nome do dataframe do planograma 
        produtos_alterar = produtos_alterar.rename(columns = {'id_produto':'good_id','nivel_par_recomendado':'par_level'})
        # Transforma tipo da coluna de 'good_id' em inteiro, para ter compatibilidade com o dataframe do planograma atual
        produtos_alterar['good_id'] = produtos_alterar['good_id'].astype(int)
        # Transformar produto em ativo
        produtos_alterar['status'] = 'active'
        # Transforma a coluna de 'good_id' no índice do dataframe
        produtos_alterar.set_index('good_id', inplace = True)
        #print(produtos_alterar)
        # Separa: produtos com par_level = 0 vão ser somente suspensos
        produtos_suspender = planograma_df[planograma_df['nivel_par_recomendado'] == 0][['id_produto']]
        # Renomeia colunas para ficarem com mesmo nome do dataframe do planograma
        produtos_suspender = produtos_suspender.rename(columns = {'id_produto':'good_id'})
        # Transforma tipo da coluna de 'good_id' em inteiro, para ter compatibilidade com o dataframe do planograma atual
        produtos_suspender['good_id'] = produtos_suspender['good_id'].astype(int)
        # Cria coluna de status para incluir o status de 'suspenso'
        produtos_suspender['status'] = 'suspended'
        # Transforma a coluna de 'good_id' no índice do dataframe
        produtos_suspender.set_index('good_id', inplace = True)
    
        ## Alterar no 'planograma_alterar' o par_level e status:
        self.planograma.update(produtos_alterar)
        self.planograma.update(produtos_suspender)
        self.fileiras = pd.DataFrame([lista_equipamentos,lista_fileiras_recomendadas,lista_fileiras_ocupadas,lista_fileiras_real]).T
        self.fileiras.columns = ['Equipamentos','Fileiras Recomendadas','Fileiras Ocupadas','Fileiras Real']
        # import pdb;pdb.set_trace()
        return self.fileiras

    def otimizarDireto(self, df=None,espaco=None, save=False, numero=-1, var=['RG'], alocar=0, abastecimento=2):
        '''
        DOCUMENTAR
        '''
        print(f'Rodando Otimização para {self.local}')
        # Pega planograma pela API
        #self.getPlanograma(numero = numero)
        # Pega base de vendas
        self.getVendas(df=df)
        # Define os espaços que serão otimizados
        self.setEspacos(espaco = espaco)
        # Carrega csv de limites de prateleira
        self.getLimites()
        # Calcula média e desvio de consumo por ponto de venda
        self.getDadosVendas(dias=30)
        # Otimiza espaços
        self.otimizar(save=save, lista_alocar_vazias=var, alocar=alocar, abastecimento=abastecimento)

    def otimizar_alocadas(self, df=None,espaco=None, save=False, numero=-1, var=['RG'], alocar=0, recomendar=0, abastecimento=2):
        '''
        DOCUMENTAR
        '''
        print(f'Rodando Otimização para {self.local}')
        # Pega planograma pela API
        # Pega base de vendas
        self.getVendas(df=df)
        # Define os espaços que serão otimizados
        self.setEspacos(espaco = espaco)
        # Carrega csv de limites de prateleira
        self.getLimites()
        # Calcula média e desvio de consumo por ponto de venda
        self.getDadosVendas(dias=30)
        # Otimiza espaços
        self.otimizar(save=save, lista_alocar_vazias=var, alocar=alocar, recomendar=recomendar, abastecimento=abastecimento)

    def analisarResultado(self):
        resultado = self.res_otim[['id_produto','produto','nivel_par_atual','nivel_par_recomendado','peso']]
        resultado['variacao'] = resultado.loc[:,'nivel_par_recomendado'] - resultado.loc[:,'nivel_par_atual']
        resultado['variacao_pct'] = resultado.loc[:,'variacao'] / resultado.loc[:,'nivel_par_atual']
        #self.produtos_depois = np.nansum(self.planograma['par_level'])
        display(self.fileiras)
        #print(f'Número de Produtos antes: {self.produtos_antes}. Número de produtos depois: {self.produtos_depois}. Variação: {self.produtos_depois - self.produtos_antes}')
        self.resultado = resultado
        #import pdb;pdb.set_trace()
        display(resultado.sort_values(by='variacao_pct', ascending=False).head(10))
        display(resultado.sort_values(by='variacao_pct').head(10))
        display(resultado.sort_values(by='nivel_par_recomendado', ascending=False).head(10))
        display(resultado[resultado['peso'] >= 4])
        return resultado

    def retorno_balanco(self, planograma_atual, produtos, chave):
        planograma = planograma_atual[["good_id", "name", "par_level", "status", "current_balance"]]
        id_produtos = produtos.loc[produtos["id"].isin(planograma.good_id.tolist())]
        id_produtos = id_produtos[["id", "name"]]
        id_produtos.columns = ["good_id", "produto"]        
        planograma = planograma.merge(id_produtos, how = 'left', on = 'good_id')
        planograma.columns = ['good_id', 'name', 'nivel_par_recomendado', 'status', 'nivel_par_atual', 'produto'] 

        if chave == "comum":
            planograma = planograma[planograma.status == "suspended"]
        else:
            planograma = planograma[((planograma.nivel_par_recomendado < planograma.nivel_par_atual) & (planograma.nivel_par_atual > 0)) | (planograma.status == "suspended")]
        
        planograma = planograma[['good_id', 'name', 'produto', 'nivel_par_recomendado', 'nivel_par_atual', 'status']]
        planograma.loc[planograma.status == "suspended", "nivel_par_recomendado"] = 0
        consumo = self.consumo[["venda_media","venda_desvio","id_produto"]]
        consumo.columns = ["venda_media","venda_desvio","good_id"]
        consumo["good_id"] = consumo["good_id"].astype(int)
        planograma = planograma.merge(consumo, how = "left", on = "good_id")
        return planograma

    def retorno_abastecimento(self, plan):
        planograma_abastecimento = plan[["name", "produto", "nivel_par_recomendado", "nivel_par_atual"]]
        planograma_abastecimento["retorno"] = planograma_abastecimento["nivel_par_atual"] - planograma_abastecimento["nivel_par_recomendado"]
        planograma_abastecimento.columns = ["Canaleta", "Produto", "Recomendado", "Quantidade Atual", "Retorno"]
        self.retornos = self.retornos.append(planograma_abastecimento)
        self.planograma_abastecimento = planograma_abastecimento
        return self.planograma_abastecimento

    def get_produtos(self):
        prod = []
        for product in vmpay.VMConnection().getProducts():
            prod.append(product)
        prod = pd.DataFrame(prod)
        return prod

    def adicionarProduto(self, novos_produtos):
        '''
        Adiciona produtos no planograma, de acordo com o dataframe do input.
        Parâmetros
        ============
        novos_produtos: Pandas DataFrame. Deve conter 'good_id' como índice e as colunas 'type','name','capacity','par_level','alert_level','desired_price' e 'status'

        Atributos alterados
        ============
        planograma: Pandas DataFrame. Alterado com novas linhas relativas aos produtos adicionados.
        '''
        novos_produtos['logical_locator'] = [self.max_ll + (i + 1) for i in novos_produtos.reset_index().index]
        # planograma_alterar = reduce(lambda left, right : pd.merge(left,right, on = ['good_id'], how = 'outer'), [temp_suspender, temp_bebida, alterar_nomes])
        # planograma_alterar['desired_price'] = np.nan
        novos_produtos = novos_produtos[['good_id','type','name','capacity','par_level','alert_level','desired_price','logical_locator','status']]
        # planograma_alterar = pd.concat([planograma_alterar], axis = 0)
        novos_produtos.set_index('good_id', inplace = True)

        ## Atualizar valores dos produtos ainda no planograma (alterações sem ser otimização)

        self.planograma = pd.concat([self.planograma,novos_produtos])

    def alterarProduto(self, alteracoes):
        '''
        Altera produtos no planograma, de acordo com o dataframe do input.
        Parâmetros
        ============
        alteracoes: Pandas DataFrame. Deve conter 'good_id' como índice e as colunas que serão alteradas.

        Atributos alterados
        ============
        planograma: Pandas DataFrame. Alterado com os novos dados de 'alteracoes'
        '''

        self.planograma.update(alteracoes)

    def mudarPreco(self,percentual, produto = None, tipo = None):
        '''
        Altera o preço de um produto específico, de um espaço inteiro (Cervejeira ou Prateleira) ou de todos os produtos do planograma.
        Parâmetros
        =============
        percentual: float. percentual de desconto / aumento que será aplicado. Para aumentos, esse valor deve ser negativo (o preço final é [preço antes x (1 - percentual)])
        produto: string. deve conter ou a sigla de um espaço (CV, CC etc.) ou a canaleta de um produto.
        tipo: string. Deve ser ou 'nome' ou 'tudo'. Se for 'nome', a alteração será feita somente no produto/equipamento conforme o atributo 'produto'. Se for 'tudo', a alteração será feita em todo o planograma.
        
        Atributos alterados
        =============
        planograma. Pandas DataFrame. Coluna de 'desired_price' alterada.
        '''
        if tipo == 'nome':
           self.planograma.loc[self.planograma['name'].str.contains(produto),'desired_price'] = self.planograma.loc[self.planograma['name'].str.contains(produto),'desired_price'] * (1-percentual)
        if tipo == 'tudo':
            self.planograma.loc[:,'desired_price'] = self.planograma.loc[:,'desired_price'] * (1-percentual)

    def salvarPlanograma(self):
        '''
        Salva csv do planograma no diretório atual.
        '''
        self.planograma.to_csv('Planograma ' + self.local + '.csv', sep = ';', decimal = ',', encoding = 'latin-1')

    def postarPlanograma(self):
        '''
        Posta planograma no sistema da vmpay.
        '''
        planograma = list(self.planograma.drop('current_balance', axis=1).reset_index().to_dict(orient='index').values())
        for x in planograma:
            if np.isnan(x['alert_level']):
                x['alert_level'] = None
            if np.isnan(x['par_level']):
                x['par_level'] = None
        
        novoplanograma=[]
        
        for i in planograma:
            if i['type']=='VirtualCoil':
                #if (isinstance(i["children"],str)) and (i["children"] != ""):
                if i["status"] == "active": 
                    novoplanograma.append({'good_id':i['good_id'],'type':i['type'],'name':i['name'],'desired_price':i['desired_price'],'logical_locator':i['logical_locator'],'status':i['status'],'children':i['children']})                    
                #import pdb;pdb.set_trace()
            else:
                novoplanograma.append({'good_id':i['good_id'],'type':i['type'],'name':i['name'],'capacity':150,'par_level':i['par_level'],'alert_level':i['alert_level'],'desired_price':i['desired_price'],'logical_locator':i['logical_locator'],'status':i['status']})
        planograma = novoplanograma

        print(self.machine_id, self.installation_id)
        vmpay.VMConnection().postPlanograms(machineID=str(self.machine_id),instaID=str(self.installation_id), planogramaFuturo=planograma)
        
        print('Planograma Postado!')

    def atualizarPlanograma(self, atualizar):
        '''
        Atualiza planograma pendente no sistema da vmpay.
        TODO conferir funcionamento
        '''
        planograma = list(atualizar.to_dict(orient='index').values())
        for x in planograma:
            if np.isnan(x['alert_level']):
                x['alert_level'] = None
            if np.isnan(x['par_level']):
                x['par_level'] = None
        vmpay.VMConnection().patchPlanograms(machineID=str(self.machine_id),instaID=str(self.installation_id), planogramaID = str(self.planograma_id), planogramaFuturo=planograma)
        print('Planograma Postado!')

    def deletarPlanograma(self):
        '''
        Deleta planograma pendente no sistema da vmpay.
        '''
        vmpay.VMConnection().delPlanograms(machineID=str(self.machine_id),instaID=str(self.installation_id), planogramaID = str(self.planograma_id))
        print('Planograma Deletado.')