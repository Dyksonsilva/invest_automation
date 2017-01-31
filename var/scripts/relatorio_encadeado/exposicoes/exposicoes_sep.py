def exposicoes_sep(id_relatorio_quaid419):

    import pandas as pd
    import numpy as np
    import pymysql as db
    import logging
    import datetime
    from var.scripts.relatorio_encadeado.exposicoes.alocacao_vertices import alocacao_vertices
    from var.scripts.relatorio_encadeado.exposicoes.indexador_fator import indexador_fator
    from var.scripts.relatorio_encadeado.exposicoes.classificacao_segmento import classificacao_segmento

    logger = logging.getLogger(__name__)

    #Declara vetores de datas
    vetor_datas_252 = [
        1,  # indexador
        21,  # 0.08
        63,
        126,
        252,
        378,
        504,
        630,
        756,
        1008,
        1260,
        2520,
        3780,  # 15
        5040,
        6300,
        7560,
        8820,
        10080,
        11340,
        12600]
    vetor_datas_360 = [
        1,
        30,
        90,
        180,
        360,
        540,
        720,
        900,
        1080,
        1440,
        1800,
        3600]
    vetor_datas = {
        "PRE": vetor_datas_252[1:13],
        "IPCA": vetor_datas_252[:1] + vetor_datas_252[2:],
        "IGPM": vetor_datas_252[:1] + vetor_datas_252[2:],
        "TR": vetor_datas_252[:1] + vetor_datas_252[2:],
        "IBOV": [1],
        "DOL": [1],
        "CCAMBIAL": vetor_datas_360,
        "COMMODITIES": [1]
    }

    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    # Arquivo operacoes
    #+' limit 100'
    query = 'SELECT * from projeto_inv.quaid_419 where id_relatorio_quaid419=' + str(id_relatorio_quaid419)+' limit 10'
    quaid_419 = pd.read_sql_query(query, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    logger.info("Tratando dados")

    quaid_419 = quaid_419[quaid_419['data_bd'] == max(quaid_419['data_bd'])]
    quaid_419 = quaid_419.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    quaid_419['EMFCODISIN'] = np.where((quaid_419['EMFCNPJFUNDO'].isin(['22307030000188'])) & (quaid_419['EMFCODISIN'].isin(['0'])), 'BRHIG3CTF005',quaid_419['EMFCODISIN'])
    quaid_419['EMFCODISIN'] = np.where((quaid_419['EMFCNPJFUNDO'].isin(['15348108000147'])) & (quaid_419['EMFCODISIN'].isin(['0'])), 'BRBRL2CTF005',quaid_419['EMFCODISIN'])

    ## Horário (para todos serem salvos com o mesmo tempo)
    horario = datetime.datetime.now()

    lista = []
    for item in vetor_datas:
        for prazo in vetor_datas[item]:
            lista.append(item + "_" + str(prazo))

    ## Adicionar vértices pora os fundos (em caso do relatório ser "G" gerencial)
    if quaid_419["tipo_relatorio"][0] == "G":
        # Separar lista de fundos fechados:
        lista_fundos = quaid_419[quaid_419["FTRCODIGO"] == "FF1"]["EMFCODISIN"].unique()
        if len(lista_fundos) > 0:
            for fundo in lista_fundos:
                vetor_datas[fundo] = 1
                lista.append(fundo)

    quaid_419_total = quaid_419

    ###Parte 1 - Gerar arquivo por tipos de produtos
    for produto in quaid_419_total["tipo_produto"].unique():

        quaid_419 = quaid_419_total[quaid_419_total["tipo_produto"] == produto]

        vetor_alocacoes = {key: 0 for key in lista}

        ##### Alocação nos vértices padrão
        for linha in range(len(quaid_419)):
            indexador = indexador_fator(quaid_419["FTRCODIGO"].iloc[linha], quaid_419["tipo_relatorio"].iloc[linha],
                                        quaid_419["EMFCODISIN"].iloc[linha])
            du = quaid_419["EMFPRAZOFLUXO"].iloc[linha]
            valor = quaid_419["EMFVLREXPRISCO"].iloc[linha] if quaid_419["TPFOPERADOR"].iloc[linha] == "+" else - \
            quaid_419["EMFVLREXPRISCO"].iloc[linha]
            taxa = quaid_419["EMFTXCONTRATADO"].iloc[linha]
            if "CTF" in indexador:
                vetor_alocacoes[indexador] += valor if valor == valor else 0
            elif "*" in indexador and quaid_419["FTRCODIGO"].iloc[linha] == "FF1":
                vetor_alocacoes[indexador] += valor if valor == valor else 0
            elif indexador in ["IBOV", "COMMODITIES", "DOL"]:
                vetor_alocacoes[indexador + "_1"] += valor if valor == valor else 0
            elif len(indexador) > 0 and indexador != "PERC":
                vertice_anterior = max(filter(lambda x: x <= du, vetor_datas[indexador]), default=0)
                vertice_posterior = min(filter(lambda x: x >= du, vetor_datas[indexador]), default=0)
                valor_anterior, valor_posterior = alocacao_vertices(valor, du, vertice_anterior, vertice_posterior)
                if vertice_anterior > 0:
                    vetor_alocacoes[str(indexador) + "_" + str(
                        vertice_anterior)] += valor_anterior if valor_anterior == valor_anterior else 0
                if vertice_posterior > 0:
                    vetor_alocacoes[str(indexador) + "_" + str(
                        vertice_posterior)] += valor_posterior if valor_posterior == valor_posterior else 0
            if indexador in ("IPCA", "IGPM", "TR", "CCAMBIAL"):
                indice = -valor
                vetor_alocacoes[str(indexador) + "_1"] += indice
            if indexador == "PERC" and taxa > 0:
                vertice_anterior = max(filter(lambda x: x <= du, vetor_datas["PRE"]), default=0)
                vertice_posterior = min(filter(lambda x: x >= du, vetor_datas["PRE"]), default=0)
                valor_anterior, valor_posterior = alocacao_vertices(valor, du, vertice_anterior, vertice_posterior)
                if vertice_anterior > 0:
                    vetor_alocacoes["PRE_" + str(vertice_anterior)] -= valor_anterior * (
                    100 - taxa) / 100 if valor_anterior == valor_anterior else 0
                if vertice_posterior > 0:
                    vetor_alocacoes["PRE_" + str(vertice_posterior)] -= valor_posterior * (
                    100 - taxa) / 100 if valor_posterior == valor_posterior else 0

        ## Transformar dict em Dataframe
        vetor_exposicoes = pd.DataFrame.from_dict(vetor_alocacoes, orient='index')

        vetor_exposicoes.reset_index(level=0, inplace=True)
        vetor_exposicoes.columns = ["vertice", "valor_exposicao"]

        ### Inserir dada_bd
        vetor_exposicoes["data_bd"] = horario

        ## Inserir identificador
        vetor_exposicoes["id_relatorio_quaid419"] = id_relatorio_quaid419

        ### tipo_alocacao pode ser Segmento e Produto
        ### categoria_alocacao: para Segmento: (RF, RV, Caixa, Derivativos, Cotas) e para Produto (tipos de produtos)

        ## Classificação categorias
        vetor_exposicoes["tipo_alocacao"] = "produto"
        vetor_exposicoes["categoria_alocacao"] = produto

        pd.io.sql.to_sql(vetor_exposicoes, name='vetor_exposicoes_sep', con=connection, if_exists="append", flavor='mysql', index=0)

    ###Parte 2 - Gerar arquivo por categorias
    for produto in quaid_419_total["ATVCODIGO"].unique():

        quaid_419 = quaid_419_total[quaid_419_total["ATVCODIGO"] == produto]

        vetor_alocacoes = {key: 0 for key in lista}

        ##### Alocação nos vértices padrão
        for linha in range(len(quaid_419)):
            indexador = indexador_fator(quaid_419["FTRCODIGO"].iloc[linha], quaid_419["tipo_relatorio"].iloc[linha],
                                        quaid_419["EMFCODISIN"].iloc[linha])
            du = quaid_419["EMFPRAZOFLUXO"].iloc[linha]
            valor = quaid_419["EMFVLREXPRISCO"].iloc[linha] if quaid_419["TPFOPERADOR"].iloc[linha] == "+" else - \
            quaid_419["EMFVLREXPRISCO"].iloc[linha]
            taxa = quaid_419["EMFTXCONTRATADO"].iloc[linha]
            if "CTF" in indexador:
                vetor_alocacoes[indexador] += valor if valor == valor else 0
            elif "*" in indexador and quaid_419["FTRCODIGO"].iloc[linha] == "FF1":
                vetor_alocacoes[indexador] += valor if valor == valor else 0
            elif indexador in ["IBOV", "COMMODITIES", "DOL"]:
                vetor_alocacoes[indexador + "_1"] += valor if valor == valor else 0
            elif len(indexador) > 0 and indexador != "PERC":
                vertice_anterior = max(filter(lambda x: x <= du, vetor_datas[indexador]), default=0)
                vertice_posterior = min(filter(lambda x: x >= du, vetor_datas[indexador]), default=0)
                valor_anterior, valor_posterior = alocacao_vertices(valor, du, vertice_anterior, vertice_posterior)
                if vertice_anterior > 0:
                    vetor_alocacoes[str(indexador) + "_" + str(
                        vertice_anterior)] += valor_anterior if valor_anterior == valor_anterior else 0
                if vertice_posterior > 0:
                    vetor_alocacoes[str(indexador) + "_" + str(
                        vertice_posterior)] += valor_posterior if valor_posterior == valor_posterior else 0
            elif indexador in ("IPCA", "IGPM", "TR", "CCAMBIAL"):
                indice = -valor
                vetor_alocacoes[str(indexador) + "_1"] += indice
            elif indexador == "PERC" and taxa > 0:
                vertice_anterior = max(filter(lambda x: x <= du, vetor_datas["PRE"]), default=0)
                vertice_posterior = min(filter(lambda x: x >= du, vetor_datas["PRE"]), default=0)
                valor_anterior, valor_posterior = alocacao_vertices(valor, du, vertice_anterior, vertice_posterior)
                if vertice_anterior > 0:
                    vetor_alocacoes["PRE_" + str(vertice_anterior)] -= valor_anterior * (
                    100 - taxa) / 100 if valor_anterior == valor_anterior else 0
                if vertice_posterior > 0:
                    vetor_alocacoes["PRE_" + str(vertice_posterior)] -= valor_posterior * (
                    100 - taxa) / 100 if valor_posterior == valor_posterior else 0

        ## Transformar dict em Dataframe
        vetor_exposicoes = pd.DataFrame.from_dict(vetor_alocacoes, orient='index')
        vetor_exposicoes.reset_index(level=0, inplace=True)
        vetor_exposicoes.columns = ["vertice", "valor_exposicao"]

        ### Inserir dada_bd
        vetor_exposicoes["data_bd"] = horario

        ## Inserir identificador
        vetor_exposicoes["id_relatorio_quaid419"] = id_relatorio_quaid419

        ### tipo_alocacao pode ser Segmento e Produto
        ### categoria_alocacao: para Segmento: (RF, RV, Caixa, Derivativos, Cotas) e para Produto (tipos de produtos)

        ## Classificação categorias
        vetor_exposicoes["tipo_alocacao"] = "segmento"
        vetor_exposicoes["categoria_alocacao"] = classificacao_segmento(produto)

        logger.info("Salvando base de dados")
        pd.io.sql.to_sql(vetor_exposicoes, name='vetor_exposicoes_sep', con=connection, if_exists="append", flavor='mysql', index=0)

        #Fecha conexão
        connection.close()