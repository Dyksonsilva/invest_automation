def processo_quaid_expo_var():

    import pymysql as db
    import pandas as pd
    import datetime
    import logging

    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import get_global_var
    from var.scripts.relatorio_encadeado.quaid419.quaid419 import quaid419
    from var.scripts.relatorio_encadeado.exposicoes.exposicoes_conso import exposicoes_conso
    from var.scripts.relatorio_encadeado.exposicoes.exposicoes_sep import exposicoes_sep
    from var.scripts.relatorio_encadeado.var_gerencial.var_gerencial import var_gerencial

    #Definitions:
    logger = logging.getLogger(__name__)
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase = dtbase[0] + '-' + dtbase[1] + '-' + dtbase[2]
    #dtbase = '2016-11-30' #Caso seja necessário forçar uma data
    horario_inicio= datetime.datetime.now()

    #Parametros gerados pelo arquivo 17-xml_quadro_operacoes_nao_org.py ao final da função parametrização
    inicio_hdi = int(get_global_var("inicio_hdi_quaid_expo_var"))
    fim_hdi = int(get_global_var("fim_hdi_quaid_expo_var"))
    inicio_gerling = int(fim_hdi)+1
    fim_gerling = int(get_global_var("fim_gerling_quaid_expo_var"))
    coent_hdi = get_global_var("coent_hdi")
    coent_gerling = get_global_var("coent_gerling")

    #HDI
    horario_processo = datetime.datetime.today()
    for i in range(inicio_hdi, fim_hdi+1):
        quaid419(i, dtbase, coent_hdi, "G", horario_processo)
        quaid419(i, dtbase, coent_hdi, "R", horario_processo)

    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * FROM count_quadros where ENTCODIGO = "'+coent_hdi+'" and tipo_relatorio = "G" and data_bd = (SELECT max(data_bd) from count_quadros where ENTCODIGO = "'+coent_hdi+'" and tipo_relatorio = "G" )'
    lista = pd.read_sql(query, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #Fecha conexão
    connection.close()

    for j in lista['id_relatorio_quaid419']:
        logger.info("Executando Relatório para HDI - ID: "+j)
        print("Executando Relatório para HDI - ID: " + j)
        exposicoes_conso(j)
        exposicoes_sep(j)
        var_gerencial(j, "normal", dtbase)
        var_gerencial(j, "estressado", dtbase)

    #Gerling
    horario_processo = datetime.datetime.today()
    for i in range(inicio_gerling, fim_gerling+1):
        quaid419(i, dtbase, coent_gerling, "G", horario_processo)
        quaid419(i, dtbase, coent_gerling, "R", horario_processo)

    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost',user='root',passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * FROM count_quadros where ENTCODIGO = "'+coent_gerling+'" and tipo_relatorio = "G" and data_bd = (SELECT max(data_bd) from count_quadros where ENTCODIGO = "'+coent_gerling+'" and tipo_relatorio = "G" )'
    lista = pd.read_sql(query, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #Fecha conexão
    connection.close()

    for j in lista['id_relatorio_quaid419']:
        exposicoes_conso(j)
        exposicoes_sep(j)
        var_gerencial(j, "normal", dtbase)
        var_gerencial(j, "estressado", dtbase)

    horario_fim = datetime.datetime.now()
    tempo=horario_fim-horario_inicio
    print(tempo)