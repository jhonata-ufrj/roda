'''
Script com funções para simular vendas e otimizar capacidade dos produtos individualmente. Hoje usada nos produtos de prateleira e geladeira branca.
'''

import numpy as np
import pandas as pd
from itertools import compress
from math import ceil, floor
from scipy.stats import genextreme

def funcao_logistica(x, L = 0.3, k = 2, x0 = 2.5):
    return (L / (1 + np.e ** (-k * (x - x0))))


def otimizar_nivel_par_simples(medias_consumo, desvios_consumo, dias, abastecimento, limites_prateleira, fileiras_ocupadas, validades, produtos_essenciais, pesos, limite_espaco, n_simulacoes = 10, parametro_regulador = 2.5, tipo = 'genextreme', alocar_vazias = False, recomendar= 0):

    '''
    Função que calcula o nível de par ótimo para cada produto, levando em consideração que em certos lugares temos um determinado número de fileiras para incluir.
    Parâmetros
    ============
    medias_consumo: lista de floats. valores das médias de consumo por produto.
    desvios_consumo: lista de floats. valores dos desvios-padrão de consumo por produto.
    dias: inteiro. numero de dias de demanda que serão simulados
    abastecimento: inteiro. número de dias que um ponto demora para ser abastecido + 1 (se o ponto é abastecido todo dia, o valor deve ser 1 e assim por diante)
    limites_prateleira: lista de inteiros. indica os múltiplos pelos quais os produtos devem ser incluídos (de 3 em 3, 6 em 6 etc.)
    validades: lista de binários. indica se o produto tem ou não validade baixa e deve receber tratamento diferente
    produtos_essenciais: lista de binários. indica se o produto é ou não essencial e deve receber tratamento diferente
    limite_espaco: inteiro. número de fileiras que devem ser preenchidas no espaço. Se for 'None', a função não vai fazer ajuste, mantendo as capacidades ótimas individuais.
    n_simulacoes: inteiro. numero de vezes que os dias de demanda serão simulados.
    parametro_regulador: float. regula o aumento de fileiras. quanto maior o valor, mais a demanda máxima tem que se aproximar do próximo múltiplo de prateleira para ter uma fileira adicionada.
    '''
    rng = np.random.default_rng()

    # Cria listas em que serão incluídas as capacidades ótimas de cada produto e as respectivas demandas máximas.
    niveis_par_otimo = []
    num_fileiras = []
    demandas_maximas = []
    
    # Para cada produto
    for i in range(len(medias_consumo)):
        # Pega média, desvio e limite de prateleira
        media_consumo = medias_consumo[i]
        desvio_consumo = desvios_consumo[i]
        limite_prateleira = limites_prateleira[i]
        validade_baixa = validades[i]
        produto_essencial = produtos_essenciais[i]
        peso = pesos[i]
        # Se limite_prateleira = NaN, transforma em limite = 1.
        if np.isnan(limite_prateleira):
            limite_prateleira = 1
        # Simula 'n_simulacoes' vezes uma demanda de 'dias' dias. Em cada simulação, calcula a demanda máxima num espaço de 2 dias e adiciona em uma lista.
        max_demanda = []

        # define parametro C
        c = funcao_logistica(media_consumo)

        for i in range(n_simulacoes):
            if tipo == 'genextreme':
                demanda = list(genextreme.rvs(c = c, loc = media_consumo, scale = desvio_consumo, size = dias))
            elif tipo == 'gumbel':
                demanda = list(rng.gumbel(loc = media_consumo, scale = desvio_consumo, size = dias))
            elif tipo == 'normal':
                demanda = list(rng.normal(loc = media_consumo, scale = desvio_consumo, size = dias))
            # Corrige as demandas menores que 0 e transforma em Pandas Series para poder aplicar o método rolling
            demanda = pd.Series([0 if i < 0 else i for i in demanda])
            # Calcula a demanda máxima em 2 dias seguidos no período simulado de 'dias' dias e adiciona esse valor à lista de max_demanda
            max_demanda.append(np.nanmax(demanda.rolling(window = abastecimento).sum()))
        # calcula capacidade ótima de acordo com a demanda máxima e os limites de prateleira. 
        # A regra é por arredondamento: demanda máxima 9 e limite 10 -> capacidade 10, demanda máxima 16 e limite 15, limite 15. 
        # Se o valor for muito próximo do limite inferior de prateleira, uso o inferior
        demanda_maxima = np.mean(max_demanda) * 1.05
        nivel_par_otimo = ceil(demanda_maxima / limite_prateleira) * limite_prateleira
        #print('Demanda Máxima: ' + str(round(demanda_maxima,2)) + ' - ' + 'Nível Par Ótimo: ' + str(nivel_par_otimo))
        # Se o valor for muito próximo do limite inferior de prateleira, usa o inferior, a menos que inferior seja 0.
        if  ((nivel_par_otimo / (demanda_maxima + 0.0001)) > parametro_regulador) and ((nivel_par_otimo / limite_prateleira) > 1): # incluindo adição de 0.0001 para evitar divisão por 0.
            nivel_par_otimo = floor(demanda_maxima / limite_prateleira) * limite_prateleira
        #    print('Novo nível par ótimo: ' + str(nivel_par_otimo))
        
        # se for produto com baixa validade, garante que a capacidade ótima não excede a média de 7 dias de consumo.
        # TODO separar em validade baixa e muito baixa
        if (validade_baixa == 1) and (media_consumo > 0):
            nivel_par_otimo = min(nivel_par_otimo, max(round((media_consumo * 7)/limite_prateleira,0) * limite_prateleira, limite_prateleira))

        ## Se for produto essencial, garante que o nivel de par recomendado é no mínimo igual a 1 fileira
        if produto_essencial == 1:
            nivel_par_otimo = max(nivel_par_otimo, limite_prateleira)

        # Inclui capacidade, número de fileiras e máximo de demanda nas listas finais.
        niveis_par_otimo.append(nivel_par_otimo)
        demandas_maximas.append(np.mean(max_demanda))
        num_fileiras.append((nivel_par_otimo / limite_prateleira) * peso)
    # Alocação de espaços restantes. Só roda se o limite de espaço for definido (diferente de None)
    if limite_espaco != None:
        # Comparar o valor indicado pela otimização e a quantidade de produtos ainda no PDV. Pega o maior para cada produto - essa é a soma de fileiras já alocadas.
        num_fileiras_real = round(sum(np.maximum(num_fileiras,fileiras_ocupadas)),0)
        
        # Define espaço 
        #  - diferença entre o limite de fileiras e o número de fileiras já ocupadas
        import pdb;pdb.set_trace()
        if alocar_vazias == 0:
            espaco_alocar = min(limite_espaco - num_fileiras_real, 0)
        else:
            espaco_alocar = limite_espaco - num_fileiras_real
            print("alocando")
        
        if recomendar == 1:
            
            #if ((sum(num_fileiras) > limite_espaco) and (sum(num_fileiras) >= 11) and (sum(num_fileiras) < 12)):
            if (sum(num_fileiras) > limite_espaco) and (sum(num_fileiras) < 12):
                limite_espaco = 12  
            espaco_alocar = limite_espaco - sum(num_fileiras)

        print(sum(num_fileiras))
        print(espaco_alocar)
        #import pdb; pdb.set_trace()
        print(f'Limite do Espaço: {limite_espaco} \nNúmero de Fileiras já ocupadas: {round(sum(fileiras_ocupadas),0)} \nNúmero de fileiras usado na comparação: {num_fileiras_real}')

        # Calcula razão do nivel de par sobre a demanda máxima.
        # Exemplo:
        # Demanda máxima do Kit Kat é de 13, capacidade é ajustada para 20. A razão nivel_par / demanda é de 1.54
        # Demanda máxima de Coca PET 2L é de 18, capacidade é ajustada para 20. A razão nivel_par / demanda é de 1.11
        razao_par_demanda = np.divide(np.array(niveis_par_otimo), np.array(demandas_maximas))
        # Se a demanda for 0, resultado vai ser infinito. Passo para 0 e transformo em lista
        razao_par_demanda[razao_par_demanda == np.nan] = 0
        razao_par_demanda = list(razao_par_demanda)
    
        #print(f'Espaço a alocar: {espaco_alocar}')
        
        limite_razoavel = round(limite_espaco / 100)

        #import pdb;pdb.set_trace()
        while (espaco_alocar > limite_razoavel) or (espaco_alocar < limite_razoavel):
            print(f"Espaço para alocar: {espaco_alocar}")
            #if recomendar == 0:
            #    return({'niveis_par':niveis_par_otimo,'max_demanda_simulada':demandas_maximas,'num_fileiras':num_fileiras})
            # import pdb;pdb.set_trace()
            # Se 'espaco_alocar' for maior ou igual a 0, inclui as fileiras ainda por alocar para os produtos com menor razao nivel_par / demanda.
            if espaco_alocar > 0:
                for i in range(int(espaco_alocar)):
                    # Ordena o array de razão nivel_par / demanda do menor para o maior
                    razao_par_demanda_ordenada = np.sort(np.array(razao_par_demanda))
                    # Tira valores 'nan'. Esses valores são de produtos com demanda = 0, que já estão com fileiras zeradas.
                    razao_par_demanda_ordenada = list(compress(razao_par_demanda_ordenada,[i > 0 for i in razao_par_demanda_ordenada]))
                    razao_par_demanda_ordenada = list(compress(razao_par_demanda_ordenada,[not np.isinf(i) for i in razao_par_demanda_ordenada]))
                    # pega a posição do produto de menor razão
                    #import pdb;pdb.set_trace()
                    x = razao_par_demanda_ordenada[0]
                    posicao_produto = razao_par_demanda.index(x)
                    num_fileiras_comp = num_fileiras[posicao_produto]
                    posicao_produto_alterado = posicao_produto
                    
                    # altera a mesma posição nas listas de número de fileiras e niveis par para adicionar 1 fileira de produtos
                    num_fileiras[posicao_produto_alterado] = num_fileiras[posicao_produto_alterado] + pesos[posicao_produto_alterado]
                    niveis_par_otimo[posicao_produto_alterado] = niveis_par_otimo[posicao_produto_alterado] + (limites_prateleira[posicao_produto_alterado]) 
                    # calcula novamente o indicador de razao_par_demanda para todos os produtos.
                    razao_par_demanda = np.divide(np.array(niveis_par_otimo), np.array(demandas_maximas))
                    # Se a demanda for 0, resultado vai ser infinito. Passo para 0 e transformo em lista
                    razao_par_demanda[razao_par_demanda == np.nan] = 0
                    razao_par_demanda = list(razao_par_demanda)
                    # Recalcula o número de fileiras a serem incluidas
                    print(f"Número de fileiras otimização: {list(num_fileiras)}")
                    #import pdb;pdb.set_trace()
                    num_fileiras_real = round(sum(np.maximum(num_fileiras,fileiras_ocupadas)),0)
                    #espaco_alocar = limite_espaco - num_fileiras_real
                    espaco_alocar -= 1
                    if (limite_espaco + 1) > sum(num_fileiras) > (limite_espaco - 1):
                        break  
            elif espaco_alocar == 0:
                break
            # Se 'espaco_alocar' for menor que 0, a ideia é tirar as fileiras a mais, começando pelos produtos com maior razão nivel_par / demanda.
            else:
                for i in range(-int(espaco_alocar)):
                    #### AJUSTE MOMENTÂNEO: NÃO QUEREMOS TIRAR PRODUTOS DAS PRATELEIRAS, ENTÃO NÃO PODEMOS TIRAR FILEIRAS DE UM PRODUTO QUE JÁ TEM SOMENTE 1 FILEIRA
                    # Para isso, em vez de olharmos só para o máximo nivel par, fazemos um for num vetor com as razoes ordenadas, e checamos se o numero de fileiras é maior que um. 
                    
                    # Ordena o array de razão nivel_par / demanda do maior para o menor
                    razao_par_demanda_ordenada = np.flip(np.sort(np.array(razao_par_demanda)))
                    # Tira valores 'nan'. Esses valores são de produtos com demanda = 0, que já estão com fileiras zeradas.
                    razao_par_demanda_ordenada = list(compress(razao_par_demanda_ordenada,[i > 0 for i in razao_par_demanda_ordenada]))
                    razao_par_demanda_ordenada = list(compress(razao_par_demanda_ordenada,[not np.isinf(i) for i in razao_par_demanda_ordenada]))
                    # Não queremos tirar produtos que tiveram alguma demanda da prateleira, então primeiro checamos se o produto tem mais de 1 fileira antes de tirar uma fileira
                    for x in razao_par_demanda_ordenada:
                        posicao_produto = razao_par_demanda.index(x)
                        num_fileiras_comp = num_fileiras[posicao_produto]
                        print("diminuir")
                        #import pdb; pdb.set_trace()
                        #print(num_fileiras_comp)
                        #print(f'Máximo de fileiras ocupadas por um produto {max(num_fileiras)}')
                        if (max(num_fileiras) > 1): # Esse if garante que o código não vai entrar em um loop infinito (se todos os produtos estiverem com só uma fileira e precisar tirar de algum lugar)
                            if num_fileiras_comp > 1:
                                posicao_produto_alterado = posicao_produto
                                break
                        else:
                            posicao_produto_alterado = posicao_produto
                            break
                    
                    # altera a mesma posição nas listas de número de fileiras e niveis par para retirar 1 fileira de produtos
                    #print(f"Número de fileiras pré-modificação {num_fileiras[posicao_produto_alterado]}")
                    num_fileiras[posicao_produto_alterado] = num_fileiras[posicao_produto_alterado] - pesos[posicao_produto_alterado]
                    #print(f"Número de fileiras pós-modificação {num_fileiras[posicao_produto_alterado]}")
                    #print(f"Nível par pré-modificação {niveis_par_otimo[posicao_produto_alterado]}")
                    niveis_par_otimo[posicao_produto_alterado] = niveis_par_otimo[posicao_produto_alterado] - (limites_prateleira[posicao_produto_alterado]) 
                    #print(f"Nível par pós-modificação {niveis_par_otimo[posicao_produto_alterado]}")
                    # calcula novamente o indicador de razao_par_demanda para todos os produtos.
                    #print(f"Razão Demanda pré modificação: {razao_par_demanda}")
                    razao_par_demanda = np.divide(np.array(niveis_par_otimo), np.array(demandas_maximas))
                    # Se a demanda for 0, resultado vai ser infinito. Passo para 0 e transformo em lista
                    razao_par_demanda[razao_par_demanda == np.inf] = 0
                    razao_par_demanda = list(razao_par_demanda)
                    #print(f"Razão Demanda pós modificação: {razao_par_demanda}")
                    # Recalcula o número de fileiras a serem incluidas
                    print(f"Número de fileiras otimização: {list(num_fileiras)}")
                    print(f"Número de fileiras já ocupadas: {list(fileiras_ocupadas)}")
                    #import pdb;pdb.set_trace()
                    num_fileiras_real = round(sum(np.maximum(num_fileiras,fileiras_ocupadas)),0)# tirando fileiras ocupadas e colocando num_fileiras_real

                    print(f'Novo número de fileiras ocupadas: {num_fileiras_real}')
                    espaco_alocar = limite_espaco - num_fileiras_real
                    #espaco_alocar -= 1
                    if (limite_espaco + 1) > sum(num_fileiras) > (limite_espaco - 1):
                        
                        break
        # Retorna dicionário com niveis_par, max_demanda_calculada e numero_de_fileiras.
    return({'niveis_par':niveis_par_otimo,'max_demanda_simulada':demandas_maximas,'num_fileiras':num_fileiras})
 
        