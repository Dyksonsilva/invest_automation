def get_anbima_carteiras(ano, mes, dia):

    import pandas as pd
    import pymysql as db
    import datetime
    import logging

    from dependencias.Metodos.funcoes_auxiliares import get_global_var

    logger = logging.getLogger(__name__)

    pagina_anbima_carteira_xml = get_global_var("pagina_anbima_carteira_xml")

    #Leitura Arquivos

    ima_geral = pd.read_excel(pagina_anbima_carteira_xml, sheetname="Geral", header=2)
    ima_irfm = pd.read_excel(pagina_anbima_carteira_xml, sheetname="IRF-M", header=2)
    ima_imab = pd.read_excel(pagina_anbima_carteira_xml, sheetname="IMA-B", header=2)
    ima_imac = pd.read_excel(pagina_anbima_carteira_xml, sheetname="IMA-C", header=2)
    ima_imas = pd.read_excel(pagina_anbima_carteira_xml, sheetname="IMA-S", header=2)

    logger.info("Leitura da página executada com sucesso")

    logger.info("Tratando dados")

    ###TRATAMENTO DO DATAFRAME IMA GERAL
    ima_geral = ima_geral.drop([3,5,7,11,13,14,16,18,19,20,21])
    
    for linha in range(len(ima_geral["Índice"])):
        ima_geral["Índice"].iloc[linha] = str(ima_geral["Índice"].iloc[linha])
        ima_geral["Unnamed: 1"].iloc[linha] = str(ima_geral["Unnamed: 1"].iloc[linha])
        if ima_geral["Índice"].iloc[linha] == 'nan':
            ima_geral["Índice"].iloc[linha] = ima_geral["Índice"].iloc[linha-1]
            
    ima_geral = ima_geral.drop(4)
            
    for linha in range(len(ima_geral["Índice"])):       
        ima_geral["Índice"].iloc[linha] = str(ima_geral["Índice"].iloc[linha])+" "+str(ima_geral["Unnamed: 1"].iloc[linha])
    
    del ima_geral["Unnamed: 1"]
    
    ima_geral.columns = ['indice',
    'data_ref', 
    'numeroindice', 
    'variacaodiariaperc', 
    'variacaomensalperc', 
    'variacaoanualperc', 
    'variacaoultimos12mesesperc',
    'variacaoultimos24mesesperc',
    'duration', 
    'valordemercado',
    'pesoperc',
    'numerodeoperacoes',
    'quantidadenegociada',
    'valornegociado']
    
    horario_bd = datetime.datetime.now()
    ima_geral["data_bd"] = horario_bd
    ima_geral = ima_geral.replace({'--': None}, regex=True)
    #######################################################################################
    
    #TRATAMENTO IMA-IRFM
    ima_irfm = ima_irfm.drop([0,1,2,3,4])
    del ima_irfm["Indice"]
    
    ima_irfm = ima_irfm.where((pd.notnull(ima_irfm)), None)
    
    ima_irfm.columns=[
    'indice',
    'titulo',
    'codigoselic',
    'isin',
    'dtvencimento',
    'taxaindicativa',
    'pu',
    'pudejuros',
    'quantidade',
    'quantidadeteorica',
    'duracao',
    'valordemercado',
    'peso',
    'numerodeoperacoes',
    'quantidadenegociada',
    'valornegociado',
    'prazo'
    ]

    horario_bd = datetime.datetime.now()
    ima_irfm["data_bd"] = horario_bd
    ima_irfm["data_ref"] = ano+mes+dia
    ima_irfm = ima_irfm.replace({'--': None}, regex=True)

    logger.info("Tratamento IMA-IRFM - OK")

    ##################################################################################################
    #####                                 Tratamento IMA-IMAB
    #################################################################################################

    ima_imab = ima_imab.drop([0,1,2,3,4])
    del ima_imab["Indice"]
    ima_imab = ima_imab.where((pd.notnull(ima_imab)), None)
    
    ima_imab.columns=[
    'indice',
    'titulo',
    'codigoselic',
    'isin',
    'dtvencimento',
    'taxaindicativa',
    'pu',
    'pudejuros',
    'quantidade',
    'quantidadeteorica',
    'duracao',
    'valordemercado',
    'peso',
    'numerodeoperacoes',
    'quantidadenegociada',
    'valornegociado',
    'prazo']
    
    horario_bd = datetime.datetime.now()
    ima_imab["data_bd"] = horario_bd
    ima_imab["data_ref"] = ano+mes+dia
    ima_irfm = ima_irfm.replace({'--': None}, regex=True)

    logger.info("Tratamento IMA-IMAB - OK")

    ##################################################################################################
    #####                                 Tratamento IMA-IMAC
    #################################################################################################

    ima_imac = ima_imac.drop([0,1,2,3,4])
    del ima_imac["Indice"]
    ima_imac = ima_imac.where((pd.notnull(ima_imac)), None)
    
    ima_imac.columns = [
    'indice',
    'titulo',
    'codigoselic',
    'isin',
    'dtvencimento',
    'taxaindicativa',
    'pu',
    'pudejuros',
    'quantidade',
    'quantidadeteorica',
    'duracao',
    'valordemercado',
    'peso',
    'numerodeoperacoes',
    'quantidadenegociada',
    'valornegociado',
    'prazo']
    
    horario_bd = datetime.datetime.now()
    ima_imac["data_bd"] = horario_bd
    ima_imac["data_ref"] = ano+mes+dia
    ima_imac = ima_imac.replace({'--': None}, regex=True)

    logger.info("Tratamento IMA-IMAC - OK")

    ##################################################################################################
    #####                                 Tratamento IMA-IMAS
    #################################################################################################

    ima_imas = ima_imas.drop([0,1,2])
    del ima_imas["Indice"]
    ima_imas = ima_imas.where((pd.notnull(ima_imas)), None)
    
    ima_imas.columns = [
    'indice',
    'titulo',
    'codigoselic',
    'isin',
    'dtvencimento',
    'taxaindicativa',
    'pu',
    'pudejuros',
    'quantidade',
    'quantidadeteorica',
    'duracao',
    'valordemercado',
    'peso',
    'numerodeoperacoes',
    'quantidadenegociada',
    'valornegociado',
    'prazo']
    
    horario_bd = datetime.datetime.now()
    ima_imas["data_bd"] = horario_bd
    ima_imas["data_ref"] = ano + mes + dia
    ima_imas = ima_imas.replace({'--': None}, regex=True)

    logger.info("Tratamento IMA-IMAS - OK")

    #############################################################

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")

    pd.io.sql.to_sql(ima_geral, name='xml_ima_geral', con=connection,if_exists="append", flavor='mysql', index=0)
    logger.info("Tabela ima_geral salva no DB com sucesso")
    pd.io.sql.to_sql(ima_irfm, name='xml_ima_irfm', con=connection,if_exists="append", flavor='mysql', index=0)
    logger.info("Tabela ima_irfm salva no DB com sucesso")
    pd.io.sql.to_sql(ima_imab, name='xml_ima_imab', con=connection,if_exists="append", flavor='mysql', index=0)
    logger.info("Tabela ima_imab salva no DB com sucesso")
    pd.io.sql.to_sql(ima_imac, name='xml_ima_imac', con=connection,if_exists="append", flavor='mysql', index=0)
    logger.info("Tabela ima_imac salva no DB com sucesso")
    pd.io.sql.to_sql(ima_imas, name='xml_ima_imas', con=connection,if_exists="append", flavor='mysql', index=0)
    logger.info("Tabela ima_imas salva no DB com sucesso")

    logger.info("Todos os dados foram salvos no DB com sucesso")

    # Fecha conexão com o banco de dados
    connection.close()