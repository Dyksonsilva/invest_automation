def get_anbima_curvas():
    
    import pandas as pd
    import pymysql as db
    import datetime
    import logging

    from dependencias.Metodos.funcoes_auxiliares import get_global_var

    logger = logging.getLogger(__name__)

    # Url da Anbima
    pagina_curvas_anbima=get_global_var("pagina_curvas_anbima")

    # Lê página da Anbima
    dados_curvas_anbima = pd.read_html(pagina_curvas_anbima, thousands=".")

    logger.info("Leitura da página executada com sucesso")
    dados_curvas_anbima = dados_curvas_anbima[4]

    logger.info("Tratando dados")
    #Padronizar nomes das colunas e colocar data_referencia
    data_referencia = dados_curvas_anbima.columns[0]
    
    dados_curvas_anbima.columns=[
    "tipo",
    "beta1",
    "beta2",
    "beta3",
    "beta4",
    "lambda1",
    "lambda2"
    ]
    
    dados_curvas_anbima["data_referencia"] = datetime.datetime.strptime(data_referencia, "%d/%m/%Y").strftime('%Y-%m-%d')

    ## Criar coluna com data_bd para a data de inserção no BD
    horario_bd = datetime.datetime.now()
    dados_curvas_anbima["data_bd"] = horario_bd
    
    ## Trocar virgula por ponto e "--"/nan por None
    
    dados_curvas_anbima = dados_curvas_anbima.replace({',':'.'}, regex=True)

    #Conexão com Banco de Dados

    logger.info("Conectando no Banco de dados")
	
    connection = db.connect('localhost', user = 'root', passwd = "root", db = 'projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")
    # Salvar na base de dados
    pd.io.sql.to_sql(dados_curvas_anbima,
                     name = 'anbima_parametros_nss',
                     con = connection,
                     if_exists = "append",
                     flavor = 'mysql',
                     index = 0)

    logger.info("Dados salvos no DB com sucesso - Tabela anbima_parametros_nss")

    # Fecha conexão com o banco de dados
    connection.close()