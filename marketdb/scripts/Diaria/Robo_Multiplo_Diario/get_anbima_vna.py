def get_anbima_vna():
    #    endereco_vna = "C:/Users/Thiago.Mazieiro/Downloads/vna_20161230.html"
    #'QUERY PARA SABER SE TEM VNA NA DATA DA POSICAO
    #SELECT * FROM projeto_inv.anbima_vna where data_referencia = "2016-11-30 00:00:00" and data_bd=(select max(data_bd) from projeto_inv.anbima_vna where data_referencia = "2016-11-30 00:00:00");

    import pandas as pd
    import pymysql as db
    import datetime
    import logging

    logger = logging.getLogger(__name__)

    endereco_vna = "http://www.anbima.com.br/vna/vna.asp"

    pagina_vna = pd.read_html(endereco_vna, thousands=".")

    logging.info("Leitura da página executada com sucesso")

    logging.info("Tratando dados")

    #Separacao de abas
    coluna1_ntn_b = pagina_vna[4][0][0:6]
    coluna2_ntn_b = pagina_vna[4][1][0:6]
    coluna1_ntn_c = pagina_vna[5][0][0:6]
    coluna2_ntn_c = pagina_vna[5][1][0:6]
    coluna1_lft = pagina_vna[6][0][0:6]
    coluna2_lft = pagina_vna[6][1][0:6]
    
    #Data referencia
    data_referencia = datetime.datetime.strptime(coluna2_ntn_b[1], "%d/%m/%Y").strftime('%Y-%m-%d')
    
    #Separar valores
    colunas =["data_referencia", "codigo_selic" , "titulo" , "vna"]
    dados_vna = pd.DataFrame(columns=colunas)
    
    resultados0 = [data_referencia, coluna1_ntn_b[3],"ntnb",coluna2_ntn_b[3]]
    resultados1 = [data_referencia, coluna1_ntn_b[4],"ntnb",coluna2_ntn_b[4]]
    resultados2 = [data_referencia, coluna1_ntn_c[3],"ntnc",coluna2_ntn_c[3]]
    resultados3 = [data_referencia, coluna1_lft[3],"lft",coluna2_lft[3]]
    
    dados_vna.loc[0]=resultados0
    dados_vna.loc[1]=resultados1
    dados_vna.loc[2]=resultados2
    dados_vna.loc[3]=resultados3
    
    # Correção valores vna
    dados_vna["vna"] = [i.replace(".","") for i in dados_vna["vna"]] 
    dados_vna["vna"] = [float(i.replace(",",".")) for i in dados_vna["vna"]] 

    # Colocar data_bd
    dados_vna["data_bd"] = datetime.datetime.now()
    
    # Substituir eventuais nan por none
    dados_vna=dados_vna.where((pd.notnull(dados_vna)), None)

    logging.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logging.info("Conexão com DB executada com sucesso")

    logging.info("Salvando base de dados")

    # Salva dados MySQL
    
    pd.io.sql.to_sql(dados_vna, name='anbima_vna', con=connection,if_exists="append", flavor='mysql', index=0)

    logging.info("Dados salvos no DB com sucesso")

    # Fecha conexão
    connection.close()
