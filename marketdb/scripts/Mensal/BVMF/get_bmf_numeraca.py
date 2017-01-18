def get_bmf_numeraca():

    import pymysql as db
    import pandas as pd
    import datetime
    import zipfile
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Retorna o path utilizado para acesso aos dados baixados
    full_path = full_path_from_database("isinp")

    z = zipfile.ZipFile(full_path + "isinp.zip", "r")
    z.extractall(path = full_path)
    z.close()

    # Ler arquivos TXT
    arquivo_numeraca = full_path + "NUMERACA.TXT"
    arquivo_emissor = full_path + "EMISSOR.TXT"

    # Lê os arquivos com a biblioteca pandas
    dados_numeraca = pd.read_csv(arquivo_numeraca, header = None, encoding = "iso-8859-1")
    dados_emissor = pd.read_csv(arquivo_emissor, header = None, encoding = "iso-8859-1")

    # Colocar nomes nas colunas
    dados_numeraca.columns =[
      "data_geracao_arquivo",
      "acao_sofrida",
      "codigo_isin",
      "codigo_emissor",
      "codigo_cfi",
      "descricao",
      "ano_emissao",
      "data_emissao",
      "ano_expiracao",
      "data_expiracao",
      "taxa_juros",
      "moedas",
      "valor_nominal",
      "preco_exercicio",
      "indexador",
      "percentual_indexador",
      "data_da_acao",
      "codigo_cetip",
      "codigo_selic",
      "codigo_pais",
      "tipo_ativo",
      "codigo_categoria",
      "codigo_especie",
      "data_base",
      "numero_emissao",
      "numero_serie",
      "tipo_emissao",
      "tipo_ativo_objeto",
      "tipo_de_entrega",
      "tipo_de_fundo",
      "tipo_de_garantia",
      "tipo_de_juros",
      "tipo_de_mercado",
      "tipo_status_isin",
      "tipo_vencimento",
      "tipo_protecao",
      "tipo_politica_distribuicao_fundos",
      "tipo_ativos_investidos",
      "tipo_forma",
      "tipo_estilo_opcao",
      "numero_serie_opcao",
      "cod_frequencia_juros",
      "situacao_isin",
      "data_primeiro_pagamento_juros"
    ]

    dados_emissor.columns = [
    "codigo_emissor",
    "nome_emissor",
    "cnpj_emissor",
    "data_criacao_emissor"
    ]

    # Substituis nan por None
    dados_numeraca = dados_numeraca.where((pd.notnull(dados_numeraca)), None)
    dados_emissor = dados_emissor.where((pd.notnull(dados_emissor)), None)

    ## Criar coluna com data_bd para a data de inserção no BD
    horario_bd = datetime.datetime.now()
    dados_numeraca["data_bd"] = horario_bd
    dados_emissor["data_bd"] = horario_bd

    connection = db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv',use_unicode=True, charset="utf8")

    pd.io.sql.to_sql(dados_numeraca, name='bmf_numeraca', con=connection, if_exists="append", flavor='mysql', index=0)
    pd.io.sql.to_sql(dados_emissor, name='bmf_emissor', con=connection, if_exists="append", flavor='mysql', index=0)

    # Fecha conexão com o banco de dados
    connection.close()
