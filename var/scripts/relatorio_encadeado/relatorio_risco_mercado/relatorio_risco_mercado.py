def relatorio_risco_mercado():

    import pandas as pd
    import pymysql as db
    import logging
    import datetime

    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import get_global_var
    from var.scripts.relatorio_encadeado.relatorio_risco_mercado.relatorio import relatorio

    #Define Variáveis iniciais
    dt_base = get_data_ultimo_dia_util_mes_anterior()
    dt_base = dt_base[0]+'-'+dt_base[1]+'-'+dt_base[2]
    dt_base = '2016-11-30'
    logger = logging.getLogger(__name__)

    cnpj_hdi = get_global_var("cnpj_hdi")

    #Conecta DB
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost',user='root',passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    #Busca lista dos fundos de primeiro nivel na carteira da HDI
    query = 'select * from projeto_inv.xml_header where cnpjcpf="' + cnpj_hdi +'" and dtposicao='+'"'+dt_base+'";'
    df = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    if len(df) == 0:
        query = 'select * from projeto_inv.xml_header where cnpj="' + cnpj_hdi +'" and dtposicao='+'"'+dt_base+'";'
        df = pd.read_sql(query, con=connection)
        logger.info("Leitura do banco de dados executada com sucesso")

    df = df.sort(['cnpj', 'cnpjcpf','data_bd'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
    df = df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    del df['index']

    #Utiliza o header da carteira da HDI como chave para a query da lista
    header_id_carteira_fundos = df.get_value(0, 'header_id').astype(str)
    lista_query = 'SELECT cnpj from projeto_inv.lista_fundos where data_bd=(select max(data_bd) from projeto_inv.lista_fundos where header_id="'+header_id_carteira_fundos+'");'
    lista_cnpj = pd.read_sql(lista_query, con=connection)
    lista=lista_cnpj['cnpj'].tolist()
    horario_bd = datetime.datetime.today()

    for cnpj in lista:
        relatorio(dt_base, cnpj, horario_bd)