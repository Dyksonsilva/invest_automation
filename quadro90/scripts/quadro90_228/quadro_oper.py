def quadro_oper(dtbase, cnpj):

    import pymysql as db
    import pandas as pd
    import logging

    logger = logging.getLogger(__name__)

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    query='select * from projeto_inv.xml_header_org where cnpjcpf="' + cnpj +'" and dtposicao='+'"'+dtbase+'";'

    df = pd.read_sql(query, con=connection)

    if len(df) ==0 :
        query ='select * from projeto_inv.xml_header_org where cnpj="' + cnpj +'" and dtposicao='+'"'+dtbase+'";'
        df = pd.read_sql(query, con=connection)
        logger.info("Leitura do banco de dados executada com sucesso")

    logger.info("Tratando dados")
    df= df.sort(['cnpj', 'cnpjcpf','data_bd'], ascending=[True, True, False])
    df= df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
    df= df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    del df['index']
    global header_id_carteira
    header_id_carteira=df.get_value(0,'header_id').astype(str)     

    #quadro de operaçoes
    query='select a.* from projeto_inv.xml_quadro_operacoes_org a right join (select header_id, max(data_bd) as data_bd from projeto_inv.xml_quadro_operacoes_org where header_id='+header_id_carteira+' group by 1) b on a.header_id=b.header_id and a.data_bd=b.data_bd;'
    qo = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    return qo