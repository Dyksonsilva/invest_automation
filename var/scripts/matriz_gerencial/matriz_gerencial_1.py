def matriz_gerencial_1():

    import pymysql as db
    import datetime
    import numpy as np
    import pandas as pd
    import logging
    import pickle

    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import get_global_var

    #Define variáveis:
    logger = logging.getLogger(__name__)
    save_path = full_path_from_database("get_output_var")
    dt_base = get_data_ultimo_dia_util_mes_anterior()
    data_final = str(dt_base[0]) + '-' + str(dt_base[1]) + '-' + str(dt_base[2])
    #data_final = '2016-11-30'
    dt_base = dt_base[0] + dt_base[1] + dt_base[2]
    #dt_base = '20161130'
    data_inicial = "2010-03-31"
    retornos_path=full_path_from_database("pickles")

    #################################################################################################################
    # VARIÁVEL PRECISA SER PARAMETRIZADA DE ACORDO COM O CÓDIGO GERADO NO SCRIPT 17-XML_QUADRO_OPERACOES_NAO_ORG.PY
    id_relatorio_quaid419 = get_global_var("id_qua419")
    #id_relatorio_quaid419 = '3650' # para novembro
    #################################################################################################################

    #Cotas cujo histórico é ruim
    lista_fundos_excluidos = ['BRBVEPCTF002','BRFCLGCTF007','BRFCLGCTF015','BROLGSCTF005','BRSNGSCTF002', 'BRMBVACTF007','BRINSBCTF020', 'BRVCCDCTF000']

    #Definição Lambdas
    lista_lambdas = {'PRE': 92, 'DIC': 94, 'DIM':85, 'DP':73, 'TP':97, "TR" : 97 , "IPCA": 94, "IGPM":86, "Dólar": 78, "Bovespa": 95, "ICB":95 }

    #Ajuste datas
    data_inicial = datetime.datetime.strptime(data_inicial, "%Y-%m-%d").date()
    data_final = datetime.datetime.strptime(data_final, "%Y-%m-%d").date()

    #Conecta no DB
    logger.info("Conectando no Banco de dados")
    connection=db.connect('localhost', user='root',passwd='root',db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    #start_time = time.time()
    #matriz_teste = pd.read_sql_query('SELECT t.* FROM ( SELECT codigo_isin, MAX(data_bd) AS max_data FROM bmf_numeraca GROUP BY codigo_isin ) AS m INNER JOIN bmf_numeraca AS t ON t.codigo_isin = m.codigo_isin AND t.data_bd = m.max_data', connection)
    #elapsed_time = time.time() - start_time
    #print (elapsed_time)

    logger.info("Tratando dados")
    # Consulta valores das séries
    dados_curvas = pd.read_sql_query('SELECT * FROM projeto_inv.curva_ettj_vertices_fixos', connection)
    dados_bacen = pd.read_sql_query('SELECT * FROM projeto_inv.bacen_series', connection)
    dados_cotas1 = pd.read_sql_query('SELECT * FROM projeto_inv.valoreconomico_cotas', connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #Remove duplicados
    dados_cotas1 = dados_cotas1.sort(['cota','dt_ref','data_bd'],ascending=[True,True,True])
    dados_cotas1 = dados_cotas1.drop_duplicates(subset=['cota','dt_ref'],take_last=True)

    data_bd = dados_cotas1['data_bd'].iloc[0]

    #Remove duplicados
    dados_bacen = dados_bacen.sort(['data_referencia','codigo','data_bd'],ascending=[True,True,True])
    dados_bacen = dados_bacen.drop_duplicates(subset=['codigo','data_referencia'],take_last=True)

    #Lê arquivo BRSND2CTF000.xlsx
    dados_cotas = pd.read_excel(full_path_from_database("excels")+'BRSND2CTF000.xlsx')
    logger.info("Arquivos lidos com sucesso")

    logger.info("Tratando dados")

    dados_cotas['dt_ref'] = dados_cotas['dt_ref'].astype(str)
    dados_cotas['dt_ref'] = dados_cotas['dt_ref'].str.split('/')
    dados_cotas['ano'] = dados_cotas['dt_ref'].str[2]
    dados_cotas['mes'] = dados_cotas['dt_ref'].str[1]
    dados_cotas['dia'] = dados_cotas['dt_ref'].str[0]
    dados_cotas['dt_ref'] = pd.to_datetime('20'+dados_cotas['ano']+dados_cotas['mes']+dados_cotas['dia']).dt.date
    del dados_cotas['ano']
    del dados_cotas['mes']
    dados_cotas['data_bd'] = None
    dados_cotas['data_bd'] = dados_cotas['data_bd'].fillna(data_bd)
    dados_cotas = dados_cotas.append(dados_cotas1)
    dados_cotas = dados_cotas.drop_duplicates(subset=['isin_fundo','dt_ref'])

    ## Filtrar datas para estimativa
    dados_curvas = dados_curvas[(dados_curvas["dt_ref"]<data_final) & (dados_curvas["dt_ref"]>data_inicial)]
    dados_bacen = dados_bacen[(dados_bacen["data_referencia"]<data_final) & (dados_bacen["data_referencia"]>data_inicial)]
    dados_cotas = dados_cotas[(dados_cotas["dt_ref"]<data_final) & (dados_cotas["dt_ref"]>data_inicial)]

    #Ajusta o nome de duas colunas
    dados_bacen['nome'] = np.where(dados_bacen['codigo']==7,'Bovespa - índice',dados_bacen['nome'])
    dados_bacen['nome'] = np.where(dados_bacen['codigo']==1,'Dólar comercial (venda)',dados_bacen['nome'])

    #Tira os índices mensais
    dados_bacen = dados_bacen[dados_bacen['frequencia']!="M"].copy()

    dados_cotas_chamber = dados_cotas[dados_cotas['isin_fundo'].isin(lista_fundos_excluidos)].copy()
    dados_cotas_chamber['codigo'] = 7
    dados_cotas_chamber = dados_cotas_chamber[['isin_fundo','codigo']].copy()
    dados_cotas_chamber = dados_cotas_chamber.drop_duplicates()

    chamber_corr = dados_bacen[dados_bacen['codigo']==7].copy()

    dados_cotas_chamber = pd.merge(chamber_corr,dados_cotas_chamber,right_on=['codigo'],left_on=['codigo'],how='left')

    dados_cotas_chamber['nome'] = dados_cotas_chamber['isin_fundo']

    for i in lista_fundos_excluidos:
        dados_cotas_chamber['codigo'] = np.where(dados_cotas_chamber['nome'].isin([i]),np.random.rand(),dados_cotas_chamber['codigo'])

    dados_cotas_chamber['codigo'] = dados_cotas_chamber['codigo']*100000
    dados_cotas_chamber['codigo'] = dados_cotas_chamber['codigo'].astype(int)

    del dados_cotas_chamber['isin_fundo']

    dados_bacen = dados_bacen.append(dados_cotas_chamber)
    dados_cotas = dados_cotas[~dados_cotas['isin_fundo'].isin(lista_fundos_excluidos)].copy()

    #Leitura e tratamento das variancias da bmfbovespa - seleciona apenas as ações da carteira
    dados_vol_bmf = pd.read_excel(full_path_from_database("bvmf")+'volatilidade_bmf_'+dt_base+'.xlsx')
    logger.info("Arquivos lidos com sucesso")

    dados_vol_bmf.columns=['codigo_negociacao','nome_empresa','preco_fechamento','ewma_anual']

    acoes_ibov = pd.read_excel(full_path_from_database("bvmf")+'Composição_ibovespa_'+dt_base+'.xlsx')
    logger.info("Arquivos lidos com sucesso")

    acoes_ibov.columns=['codigo','1','2','3','4']
    acoes_ibov['codigo'] = acoes_ibov['codigo']

    quaid_419 = pd.read_sql_query('SELECT * from projeto_inv.quaid_419 where id_relatorio_quaid419='+str(id_relatorio_quaid419) +' and FTRCODIGO = "AA1"', connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    quaid_419 = quaid_419[quaid_419['data_bd']==max(quaid_419['data_bd'])]
    quaid_419 = quaid_419.rename(columns={'EMFCODCUSTODIA':'codigo','EMFCODISIN':'2'})
    quaid_419 = quaid_419[['codigo','2']].copy()

    acoes_ibov = acoes_ibov.append(quaid_419)

    lista_acoes = acoes_ibov['codigo'].unique()

    dados_vol_bmf = dados_vol_bmf[dados_vol_bmf.codigo_negociacao.isin(lista_acoes)].copy()

    dados_vol_bmf['codigo'] = 7

    del dados_vol_bmf['nome_empresa']
    del dados_vol_bmf['preco_fechamento']
    del dados_vol_bmf['ewma_anual']

    chamber_corr = dados_bacen[dados_bacen['codigo']==7].copy()

    dados_vol_bmf = pd.merge(chamber_corr,dados_vol_bmf,right_on=['codigo'],left_on=['codigo'],how='left')
    dados_vol_bmf['nome'] = dados_vol_bmf['codigo_negociacao'] + '_1'

    for i in lista_acoes:
        dados_vol_bmf['codigo'] = np.where(dados_vol_bmf['nome'].isin([i+'_1']),np.random.rand(),dados_vol_bmf['codigo'])

    dados_vol_bmf['codigo'] = dados_vol_bmf['codigo']*100000
    dados_vol_bmf['codigo'] = dados_vol_bmf['codigo'].astype(int)

    del dados_vol_bmf['codigo_negociacao']

    dados_bacen = dados_bacen.append(dados_vol_bmf)

    ## Lista de séries do Bacen a ser utlizada
    aux1 = dados_vol_bmf[['codigo','nome']].copy()
    aux1 = aux1.drop_duplicates()
    aux2 = dados_cotas_chamber[['codigo','nome']].copy()
    aux2 = aux2.drop_duplicates()
    aux2 = aux2.append(aux1)
    lista_series_bacen = aux2['codigo'].tolist()

    #lista_series_bacen = lista_series_bacen.tolist()
    n_len = len(lista_series_bacen)
    lista_series_bacen.insert(n_len,1)
    n_len = len(lista_series_bacen)
    lista_series_bacen.insert(n_len,7)

    ## Ajuste para variáveis virarem colunas
    tabela_ajustada= pd.pivot_table(dados_curvas,index=["dt_ref"], columns=["indexador_cod","prazo"], values =["tx_spot_ano"])

    ## Ajuste data Cupom Cambial (de 252 para 360)
    nova_lista=[]
    for i in range(len(tabela_ajustada.columns)):
        if tabela_ajustada.columns[i][1]=="DP":
            resultado = (tabela_ajustada.columns[i][0],"DP",int(tabela_ajustada.columns[i][2]*(360/252.)))
        else:
            resultado = tabela_ajustada.columns[i]
        nova_lista.append(resultado)

    tabela_ajustada.columns = nova_lista

    ## Ajuste data para PU
    tabela_pu = tabela_ajustada
    for coluna in tabela_ajustada.columns:
        periodo = coluna[2]
        taxa = coluna[1]
        if taxa == "DP":
            fator_data = (periodo/360.)
        else:
            fator_data = (periodo/252.)
        tabela_pu[coluna] = 100/((1+tabela_ajustada[coluna])**fator_data)

    #Guarda valores para a proxy das cotas
    proxy_cotas = dados_bacen[['data_referencia','codigo','valor']][dados_bacen["codigo"]==7].copy()

    teste = tabela_pu
    for serie in lista_series_bacen:
        try:
            dados_serie = dados_bacen[dados_bacen["codigo"]==serie]
            dados_serie = dados_serie.drop_duplicates(subset=['data_referencia','codigo'])
            teste = pd.merge(teste, dados_serie[["valor","data_referencia"]], left_index = True, right_on =["data_referencia"], how ="left")
            teste.rename(columns = {'valor':dados_serie["nome"].iloc[0]}, inplace=True)
            teste.index = teste["data_referencia"]
            del teste["data_referencia"]
        except:
            pass

    #Ajustar a proxy das cotas - Ibovespa - serie historica que esta no bacen
    dados_cotas_proxy = pd.DataFrame(columns=['codigo','isin_fundo'])
    dados_cotas_proxy1 = []

    lista_isin_fundo = dados_cotas['isin_fundo'].unique()
    for i in lista_isin_fundo:
        dados_cotas_proxy1.append(i)

    dados_cotas_proxy['isin_fundo'] = dados_cotas_proxy1
    dados_cotas_proxy['codigo'] = 7

    #Dropa duplicatas bacen
    proxy_cotas = proxy_cotas.drop_duplicates(subset=['data_referencia','codigo'])
    proxy_cotas.columns = ['dt_ref','codigo','cota']

    dados_cotas_proxy = pd.merge(dados_cotas_proxy,proxy_cotas,left_on=['codigo'],right_on=['codigo'],how='left')
    dados_cotas = dados_cotas_proxy[['dt_ref','isin_fundo','cota']].copy()

    tabela_cotas = pd.pivot_table(dados_cotas,index=["dt_ref"], columns=["isin_fundo"], values =["cota"])

    teste = teste.merge(tabela_cotas,left_index=True, right_index=True)

    ## Preencher datas vazias com o último valor disponível
    teste.fillna(method='pad', inplace=True)

    ## Preencher o histórico de nan com o primeiro valor da série
    teste.fillna(method='bfill', inplace=True)

    ## Cálculo do retorno diário
    retornos = teste.pct_change()

    ## Retirar primeira observação NaN
    retornos = retornos[1:]

    aux = retornos**2
    aux.to_excel(save_path+'retornos'+str(data_final)+'.xlsx')
    logger.info("Arquivos salvos com sucesso")

    connection.close()

    pickle_file=open(retornos_path+'matriz_gerencial_retornos'+".pkl","w")
    pickle.dump(retornos,pickle_file)
    pickle_file.close()