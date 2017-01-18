
def get_bmf_cotacoes_hist():

    import pandas as pd
    import pymysql as db
    import datetime
    import zipfile
    import logging

    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    logger = logging.getLogger(__name__)

    # Retorna um array (ano, mes e dia) referente ao útimo dia útil do mês anterior configurado no banco de dados
    array_data = get_data_ultimo_dia_util_mes_anterior()

    # Retorna o path utilizado para acesso aos dados baixados
    full_path = full_path_from_database("cotacoes")

    full_path = full_path + "COTAHIST_D" + str(array_data[2]) + str(array_data[1]) + str(array_data[0])

    # Acesso ao arquivo zip
    z = zipfile.ZipFile(full_path + ".zip","r")
    z.extractall(path = full_path)

    logger.info("Arquivos extraidos com sucesso em :"+full_path)

    # Fechamento diário
    tamanho_campos = [2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]
    arquivo_bovespa = full_path + "/COTAHIST_D" + str(array_data[2]) + str(array_data[1]) + str(array_data[0]) +".txt"
    dados_acoes = pd.read_fwf(arquivo_bovespa, widths = tamanho_campos, header=0)

    logger.info("Leitura da página executada com sucesso")

    logger.info("Tratando dados")

    #Padronizar nomes das colunas
    dados_acoes.columns = [
    "tipo_registro",
    "data_pregao",
    "cod_bdi",
    "cod_negociacao",
    "tipo_mercado",
    "noma_empresa",
    "especificacao_papel",
    "prazo_dias_merc_termo",
    "moeda_referencia",
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
    "numero_negocios",
    "quantidade_papeis_negociados",
    "volume_total_negociado",
    "preco_exercicio",
    "ìndicador_correcao_precos",
    "data_vencimento" ,
    "fator_cotacao",
    "preco_exercicio_pontos",
    "codigo_isin",
    "num_distribuicao_papel"]

    # Eliminar a última linha ()
    linha = len(dados_acoes["data_pregao"])
    dados_acoes = dados_acoes.drop(linha-1)

    # Colocar valores com virgula (divir por 100)

    lista_virgula=[
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
    "volume_total_negociado",
    "preco_exercicio",
    "preco_exercicio_pontos"
    ]

    for coluna in lista_virgula:
        dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]

    # Warnings tamanho
    #warnings.simplefilter(action = "ignore", category = RuntimeWarning)

    horario_bd = datetime.datetime.now()
    horario_bd = horario_bd.strftime("%Y-%m-%d %H:%M:%S")

    dados_acoes['data_bd'] = horario_bd

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")

    pd.io.sql.to_sql(dados_acoes, name='bovespa_cotahist',
                     con=connection,
                     if_exists="append",
                     flavor='mysql',
                     index=0,
                     chunksize=5000)

    logger.info("Dados salvos no DB com sucesso")

    connection.close()
