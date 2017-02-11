def upload_manual_cotas(query, columns, table):

    import os
    import pandas as pd
    import pymysql as db
    import datetime
    import numpy as np
    import logging

    from findt import FinDt
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    logger = logging.getLogger(__name__)

    # Definindo os paths
    path_feriados = full_path_from_database("feriados_nacionais") + "feriados_nacionais.csv"
    path_cotas = full_path_from_database("bloomberg_cotas")

    #Criação série diária
    logger.info("Tratando dados")

    #Seleciona o última dia do mês vigente
    mesfim = datetime.date.today().month
    fim = datetime.date(datetime.date.today().year,mesfim,1)-pd.DateOffset(months=0, days=1)

    dt_ref = pd.date_range (start='01/01/1996', end=fim, freq='D').date
    ano = pd.date_range (start='01/01/1996', end=fim, freq='D').year
    mes = pd.date_range (start='01/01/1996', end=fim, freq='D').month
    dias = pd.date_range (start='01/01/1996', end=fim, freq='D').day
    serie_dias = pd.DataFrame(columns=['dt_ref', 'ano', 'mes','dia'])
    serie_dias['dt_ref']=dt_ref
    serie_dias['ano']=ano
    serie_dias['mes']=mes
    serie_dias['dia']=dias

    # Identificar se é dia útil
    dt_max = max(serie_dias['dt_ref'])
    dt_min = min(serie_dias['dt_ref'])
    per = FinDt.DatasFinanceiras(dt_min, dt_max, path_arquivo=path_feriados)

    du = pd.DataFrame(columns=['dt_ref'])
    du['dt_ref'] = per.dias(3)
    du['du_1'] = 1

    serie_dias = serie_dias.merge(du, on=['dt_ref'], how='left')
    serie_dias['du_1'] = serie_dias['du_1'].fillna(0)
    serie_dias['dc_1'] = 1

    # Calculo de dias corridos por mes
    serie_dias_group_count = serie_dias[['dt_ref','ano','mes']].groupby(['ano','mes']).agg(['count'])
    serie_dias_group_count = serie_dias_group_count.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    serie_dias_group_count_filter = pd.DataFrame(columns=['ano', 'mes', 'dc'])

    serie_dias_group_count_filter['ano'] = serie_dias_group_count['ano']
    serie_dias_group_count_filter['mes'] = serie_dias_group_count['mes']
    serie_dias_group_count_filter['dc'] = serie_dias_group_count['dt_ref']

    serie_dias = serie_dias.merge(serie_dias_group_count_filter, on = ['ano', 'mes'], how='left')

    #calculo de dias uteis por mes
    serie_dias_group_sum = serie_dias[['du_1','ano','mes']].groupby(['ano','mes']).agg(['sum'])
    serie_dias_group_sum = serie_dias_group_sum.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    serie_dias_group_sum_filter = pd.DataFrame(columns=['ano', 'mes', 'du'])

    serie_dias_group_sum_filter['ano'] = serie_dias_group_sum['ano']
    serie_dias_group_sum_filter['mes'] = serie_dias_group_sum['mes']
    serie_dias_group_sum_filter['du'] = serie_dias_group_sum['du_1']

    serie_dias = serie_dias.merge(serie_dias_group_sum_filter, on = ['ano', 'mes'], how='left')

    #Criação da série de cotas

    lista_cotas = os.listdir(path_cotas)

    lista_cotas = [ x for x in lista_cotas if "~" not in x ]

    cotasdf = pd.DataFrame()

    for i in lista_cotas:
        leitura = pd.read_excel(path_cotas + '/' + i)
        leitura = leitura.rename(columns={'Date':'dt_ref','FUND_NET_ASSET_VAL':'cota','FUND_TOTAL_ASSETS':'pl'})
        leitura['isin_fundo'] = i
        cotasdf = cotasdf.append(leitura)

    cotasdf['isin_fundo'] = cotasdf['isin_fundo'].str.split('.')
    cotasdf['isin_fundo'] = cotasdf['isin_fundo'].str[0]

    cotasdf['dt_ref'] = cotasdf['dt_ref'].astype(str)
    cotasdf['dt_ref'] = cotasdf['dt_ref'].str.split('-')
    cotasdf['ano'] = cotasdf['dt_ref'].str[0]
    cotasdf['mes'] = cotasdf['dt_ref'].str[1]
    cotasdf['dia'] = cotasdf['dt_ref'].str[2]
    cotasdf['dt_ref'] = pd.to_datetime(cotasdf['ano'] + cotasdf['mes'] + cotasdf['dia']).dt.date
    del cotasdf['ano']
    del cotasdf['mes']
    del cotasdf['dia']

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    cnpj = pd.read_sql_query(query, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    cnpj['cnpjfundo_outros'] = np.where(cnpj['cnpjfundo_outros'].isnull(),cnpj['cnpjfundo_1nivel'],cnpj['cnpjfundo_outros'])
    cnpj['cnpj'] = cnpj['cnpjfundo_outros']
    del cnpj['cnpjfundo_1nivel']
    del cnpj['cnpjfundo_outros']

    cnpj = cnpj.drop_duplicates()

    cotasdf = pd.merge(cotasdf,cnpj,left_on='isin_fundo',right_on='isin',how='left')
    del cotasdf['isin']

    cotasdf = cotasdf.rename(columns=columns)
    cotasdf['data_bd'] = datetime.datetime.today()

    logger.info("Salvando base de dados")
    pd.io.sql.to_sql(cotasdf, name=table, con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)

    connection.close()