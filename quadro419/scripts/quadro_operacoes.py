def quadro_operacoes(tipo_seguradora):

    import pandas as pd
    import numpy as np
    import pymysql as db
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_global_var

    ################################################################################
    ## Parâmetros para gerar o relatório
    ################################################################################

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]
    dt_base = dtbase[0] + '-' + dtbase[1] + '-' + dtbase[2]

    end_val = full_path_from_database('get_output_quadro419')

    if tipo_seguradora == 'hdi':
        id_relatorio_qo = int(get_global_var("id_relatorio_qo_hdi")) # Buscar esse valor na coluna id_relatorio_qo no Excel do quadro consolidado
        ent_codigo = get_global_var("coent_hdi")

    if tipo_seguradora == 'gerling':
        id_relatorio_qo = int(get_global_var("id_relatorio_qo_gerling")) # Buscar esse valor na coluna id_relatorio_qo no Excel do quadro consolidado
        ent_codigo = get_global_var("coent_gerling")

    tipo_relatorio = "R"  ##Preencher se o relatório é regulatório (R) ou gerencial (G)

    # Fluxos de pagamento
    ano_mtm = dtbase[0]
    mes_mtm = dtbase[1]
    dia_mtm = dtbase[2]

    ################################################################################
    # Fontes de dados
    ################################################################################

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    # Arquivo operacoes
    arquivo_operacoes = pd.read_sql_query(
        'SELECT * from projeto_inv.xml_quadro_operacoes where id_relatorio_qo=' + str(id_relatorio_qo), connection)

    # Retira as compromissadas
    # compromissados = arquivo_operacoes[['isin','produto']][arquivo_operacoes['produto'].str.contains('compromissada')]
    # compromissados['marker']=1
    # arquivo_operacoes = pd.merge(arquivo_operacoes,compromissados,left_on=['isin','produto'],right_on=['isin','produto'],how='left')
    # arquivo_operacoes = arquivo_operacoes[arquivo_operacoes['marker'].isnull()].copy()
    # del arquivo_operacoes['marker']

    # Altera o indexador dos títulos públicos que possam vir errados
    arquivo_operacoes['indexador'] = np.where(
        (arquivo_operacoes['produto'].isin(['titulo público'])) & (arquivo_operacoes['ativo'].isin(['760100', '760199'])),
        'IAP', arquivo_operacoes['indexador'])
    arquivo_operacoes['indexador'] = np.where(
        (arquivo_operacoes['produto'].isin(['titulo público'])) & (arquivo_operacoes['ativo'].isin(['770100'])), 'IGM',
        arquivo_operacoes['indexador'])
    arquivo_operacoes['indexador'] = np.where(
        (arquivo_operacoes['produto'].isin(['titulo público'])) & (arquivo_operacoes['ativo'].isin(['210100'])), 'SEL',
        arquivo_operacoes['indexador'])


    query_fluxos = 'select a.codigo_isin, a.perc_mtm, a.tipo_ativo, a.prazo_du, a.indexador, a.taxa_juros, a.data_mtm, a.data_bd from projeto_inv.mtm_renda_fixa a right join (select codigo_isin, data_mtm, max(data_bd) as data_bd from projeto_inv.mtm_renda_fixa where data_mtm="' + dtbase_concat + '" group by 1,2) b on a.codigo_isin=b.codigo_isin and a.data_mtm=b.data_mtm and a.data_bd=b.data_bd;'
    fluxos = pd.read_sql_query(query_fluxos, connection)

    ###############################################################################
    ###############################################################################
    ###############################################################################
    #
    #  AMBIENTE DA CORA!!!! - CORREÇÃO FLUXO TÍTULOS PÚBLICOS
    #
    # PROBLEMA IDENTIFICADO:
    # FLUXO DAS NTNB (PELO MENOS) SEM PAGAMENTO DE CUPOM. VALOR DE MERCADO PAGO
    # TODO NO VENCIMENTO

    fluxos1 = fluxos.drop_duplicates(subset=['codigo_isin', 'prazo_du'])
    fluxos1[fluxos1['tipo_ativo'] == 'TPF']
    fluxos[fluxos['tipo_ativo'] == 'TPF']
    ###############################################################################
    ###############################################################################
    ###############################################################################

    # Separação da taxa_operação vindo de cadastro
    indexadores = fluxos[['codigo_isin', 'taxa_juros', 'tipo_ativo']].copy()
    indexadores = indexadores.drop_duplicates(subset=['codigo_isin'])
    indexadores = indexadores[indexadores['tipo_ativo'] != 'TPF'].copy()

    arquivo_operacoes = pd.merge(arquivo_operacoes, indexadores, left_on=['isin'], right_on=['codigo_isin'], how='left')
    arquivo_operacoes['tx_operacao'] = np.where(arquivo_operacoes['tipo_ativo'].notnull(), arquivo_operacoes['taxa_juros'],
                                                arquivo_operacoes['tx_operacao'])

    del arquivo_operacoes['codigo_isin']
    del arquivo_operacoes['taxa_juros']
    del arquivo_operacoes['tipo_ativo']


    ################################################################################
    ## Códigos auxiliares
    ################################################################################

    ## Código para encontrar o ATVCODIGO dada a descrição do produto
    def atvcodigo(linha):
        if linha == "debênture":
            return "A1001"
        elif linha == "caixa":
            return "A0001"
        elif linha == "título público":
            return "A1001"
        elif "ompromissada" in linha:
            return "A1005"
        elif "blico" in linha:
            return "A1001"
        elif linha == "título privado":
            return "A1001"
        elif linha == "ações":
            return "A1002"
        elif linha == "valores a receber":
            return "A0001"
        elif linha == "swap":
            return "D0003"
        elif linha == "Futuro":
            return "D0001"
        elif "termo" in linha:
            return "D0002"
        elif "Termo" in linha:
            return "D0002"
        elif "juste" in linha:
            return "D0001"
        elif linha == "fundo":
            return "A1003"
        elif "espesas" in linha:
            return "A0001"
        elif "Provisão" in linha:
            return "A0001"
        elif "tributos" in linha:
            return "A0001"
        elif "Corretagem" in linha:
            return "A0001"
        elif "resgatar" in linha:
            return "A0001"
        elif "emitir" in linha:
            return "A0001"
        elif "pagar" in linha:
            return "A0001"
        elif "receber" in linha:
            return "A0001"
        elif "futuro" in linha:
            return "D0001"


    ### Código para dar o TPFOPERADOR

    def tpfoperador(linha):
        if linha == "V":
            return "-"
        else:
            return "+"


    ### Código para dar o TPECODIGO
    def tpecodigo(linha):
        if "blico" in linha:
            return "PU01"
        else:
            return "PR01"


    ### Código para dar o LCRCODIGO
    def lcrcodigo(linha):
        if "blico" in linha:
            return "N01"
        elif "privado" in linha:
            return "N02"
        elif "uturo" in linha:
            return "N03"
        elif "swap" in linha:
            return "N03"
        elif "ermo" in linha:
            return "N03"
        elif "ações":
            return "N04"
        else:
            return "N05"


    ### Código para dar o FTRCODIGO
    def ftrcodigo(numero_linha):
        if arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and arquivo_operacoes["tx_operacao"].iloc[
            numero_linha] == 0:
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and arquivo_operacoes["tx_operacao"].iloc[
            numero_linha] != 0:
            return "TD2"
        elif "undo" in arquivo_operacoes["produto"].iloc[numero_linha]:
            return "FF1"
        elif "DI1" in str(arquivo_operacoes["ativo"].iloc[numero_linha]) and "uturo" in arquivo_operacoes["produto"].iloc[
            numero_linha]:
            return "JJ1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and np.isnan(
                arquivo_operacoes["tx_operacao"].iloc[numero_linha]):
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IAP":
            return "JI1"
        # elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IPCA":
        #        return "JI1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "ANB":
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IPC":
            return "JI1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IGM":
            return "JI2"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "SEL":
            return "TS1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "ações":
            return "AA1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "uturo" and "IND" in arquivo_operacoes["isin"].iloc[
            numero_linha]:
            return "AA1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "uturo" and "DOL" in arquivo_operacoes["isin"].iloc[
            numero_linha]:
            return "ME1"
        else:
            return "JJ1"


    ################################################################################
    ## Gerar arquivo base
    ################################################################################
    colunas = ["EMFSEQ",
               "ENTCODIGO",
               "MRFMESANO",
               "QUAID",
               "ATVCODIGO",
               "TPFOPERADOR",
               "FTRCODIGO",
               "LCRCODIGO",
               "TCTCODIGO",
               "TPECODIGO",
               "EMFPRAZOFLUXO",
               "EMFVLREXPRISCO",
               "EMFCNPJFUNDO",
               "EMFCODISIN",
               "EMFCODCUSTODIA",
               "EMFMULTIPLOFATOR",
               "EMFTXCONTRATADO",
               "EMFTXMERCADO",
               "TPFOPERADORDERIVATIVO",
               "EMFVLRDERIVATIVO",
               "EMFCODGRUPO",
               "tipo_produto"]

    quaid_419 = pd.DataFrame(columns=colunas)

    for linha in range(len(arquivo_operacoes)):
        posicao = {}
        posicao["EMFSEQ"] = linha + 1
        posicao["ENTCODIGO"] = ent_codigo
        posicao["MRFMESANO"] = dtbase_concat
        posicao["QUAID"] = 419
        posicao["ATVCODIGO"] = atvcodigo(arquivo_operacoes["produto"].iloc[linha])
        posicao["TPFOPERADOR"] = tpfoperador(arquivo_operacoes["a_p_op"].iloc[linha])
        posicao["FTRCODIGO"] = ftrcodigo(linha)
        posicao["LCRCODIGO"] = lcrcodigo(arquivo_operacoes["produto"].iloc[linha])
        posicao["EMFCNPJFUNDO"] = arquivo_operacoes["cnpjfundo_1nivel"].iloc[linha]
        posicao["TCTCODIGO"] = "02" if posicao["EMFCNPJFUNDO"] == None else "01"
        posicao["TPECODIGO"] = tpecodigo(arquivo_operacoes["produto"].iloc[linha])
        posicao["EMFPRAZOFLUXO"] = arquivo_operacoes["du"].iloc[linha]
        posicao["EMFVLREXPRISCO"] = arquivo_operacoes["mtm_mercado"].iloc[linha] if \
        arquivo_operacoes["caracteristica"].iloc[linha] == "V" else arquivo_operacoes["mtm_info"].iloc[linha]
        posicao["EMFCODISIN"] = arquivo_operacoes["isin"].iloc[linha]
        posicao["EMFCODCUSTODIA"] = arquivo_operacoes["ativo"].iloc[linha]
        posicao["EMFMULTIPLOFATOR"] = 0  # Ajuste posterior para exposições com mais de 1 fator de risco
        posicao["EMFTXCONTRATADO"] = arquivo_operacoes["perc_index"].iloc[linha] if posicao["FTRCODIGO"] in (
        "TS1", "TD1") else 0
        posicao["EMFTXMERCADO"] = 100 if posicao["EMFTXCONTRATADO"] != 0 else 0
        posicao["TPFOPERADORDERIVATIVO"] = 0
        posicao["EMFVLRDERIVATIVO"] = 0
        posicao["EMFCODGRUPO"] = 0
        posicao["tipo_produto"] = arquivo_operacoes["produto"].iloc[linha]
        ## Ajuste tipo_relatorio: se for gerencial (G) não considerar papéis marcados na curva (Vencimento)
        if tipo_relatorio == "G" and arquivo_operacoes["caracteristica"].iloc[linha] == "V":
            posicao = {}
        quaid_419.loc[linha] = pd.Series(posicao)

    ## Limpar linhas vazias
    quaid_419.dropna(how='all', inplace=True)

    ################################################################################
    ## Abertura fluxos de pagamento
    ################################################################################

    # quaid_419["EMFCODISIN"].fillna("", inplace=True)
    ## Lista de ISINs
    lista_isin = quaid_419["EMFCODISIN"].unique()

    ## Filtrar os fluxos dos ISINs
    fluxos_isin = fluxos[fluxos["codigo_isin"].isin(lista_isin)]

    ## Substituicao Fluxos
    if len(fluxos_isin) > 0:
        compromissada = quaid_419[quaid_419['tipo_produto'].str.contains('compromissada')]
        quaid_419 = quaid_419[(quaid_419['tipo_produto'] != "compromissada: titulo público") & (
        quaid_419['tipo_produto'] != "compromissada - debênture")]

        teste = pd.merge(quaid_419[quaid_419["ATVCODIGO"] != "D0002"], fluxos, left_on=["EMFCODISIN"],
                         right_on=["codigo_isin"], how="left")
        teste = teste.append(compromissada)

        ## Calcular valor exposição e dias úteis
        # teste["EMFVLREXPRISCO"] = teste["perc_mtm"].isnull().map({True:teste["EMFVLREXPRISCO"],False:teste["perc_mtm"]*teste["EMFVLREXPRISCO"]})
        # teste["EMFPRAZOFLUXO"]= teste["perc_mtm"].isnull().map({True:teste["EMFPRAZOFLUXO"],False:teste["prazo_du"]})

        teste["EMFVLREXPRISCO"] = np.where(np.isnan(teste["perc_mtm"]), teste["EMFVLREXPRISCO"],
                                           teste["perc_mtm"] * teste["EMFVLREXPRISCO"])
        teste["EMFPRAZOFLUXO"] = np.where(np.isnan(teste["perc_mtm"]), teste["EMFPRAZOFLUXO"], teste["prazo_du"])

        teste["EMFPRAZOFLUXO"] = np.where(teste["tipo_produto"].isin(["ações"]), 1, teste["EMFPRAZOFLUXO"])
        teste["EMFPRAZOFLUXO"] = np.where(teste["tipo_produto"].str.contains('compromissada'), 1, teste["EMFPRAZOFLUXO"])

        teste["FTRCODIGO"] = np.where(
            (teste["tipo_ativo"].isin(['CTF'])) & (teste["indexador"].isin(['IPCA', 'IAP', 'IPC'])), 'JI1',
            teste["FTRCODIGO"])
        teste["FTRCODIGO"] = np.where((teste["tipo_ativo"].isin(['CTF'])) & (teste["indexador"].isin(['DI1', 'DI', 'CDI'])),
                                      'TD1', teste["FTRCODIGO"])
        teste["FTRCODIGO"] = np.where(
            (teste["tipo_produto"].str.contains('compromissada')) & (teste["indexador"].isin(['PRE'])), 'JJ1',
            teste["FTRCODIGO"])
        teste["EMFPRAZOFLUXO"] = np.where(teste['FTRCODIGO'].isin(['FF1']), 1, teste["EMFPRAZOFLUXO"])

        ## Eliminar colunas do merge
        #    teste.drop(teste.columns[21:123],axis=1,inplace=True)
        del teste['codigo_isin']
        del teste['perc_mtm']
        del teste['taxa_juros']
        del teste['tipo_ativo']
        del teste['prazo_du']
        del teste['data_bd']
        del teste['data_mtm']
        del teste['indexador']

        ##################################
        # NA MAO NERVOSO!
        #################################
        teste['EMFPRAZOFLUXO'] = np.where(teste['EMFCODISIN'] == 'BRSTNCLTN6W5', 1, teste['EMFPRAZOFLUXO'])

        ## Eliminar linhas dos fluxos anteriores a data de referência
        teste['EMFPRAZOFLUXO'] = teste['EMFPRAZOFLUXO'].fillna(1)
        teste = teste[teste["EMFPRAZOFLUXO"] > 0]

        ## Voltar linhas D0002
        teste = teste.append(quaid_419[quaid_419["ATVCODIGO"] == "D0002"], ignore_index=True)
        quaid_419 = teste

    ##################################################################################
    ## Ajustar Futuros
    ##################################################################################

    futuros = quaid_419[quaid_419["ATVCODIGO"] == "D0001"]

    isins = futuros["EMFCODISIN"].unique()
    cnpjs = futuros["EMFCNPJFUNDO"].unique()
    prazos = futuros["EMFPRAZOFLUXO"].unique()
    indexadores = futuros["FTRCODIGO"].unique()  ############## INSERI ESTA LISTA ##################

    futuros_ajustados = pd.DataFrame()
    for cnpj in cnpjs:
        for isin in isins:
            for prazo in prazos:
                for indexador in indexadores:  ############## INSERI ESSE FOR
                    lista = futuros[(futuros["EMFCODISIN"] == isin) & (futuros["EMFCNPJFUNDO"] == cnpj) & (
                    futuros["EMFPRAZOFLUXO"] == prazo) & (futuros[
                                                              "FTRCODIGO"] == indexador)]  ################ INSERI & (futuros["FTRCODIGO"]==indexador)
                    if len(lista) < 1:
                        pass
                    else:
                        valor_exprisco = sum(lista[lista.EMFCODCUSTODIA.notnull()]["EMFVLREXPRISCO"])
                        valor_ajuste = sum(lista[lista.EMFCODCUSTODIA.isnull()]["EMFVLREXPRISCO"])
                        linha_final = lista[lista.EMFCODCUSTODIA.notnull()]
                        linha_final["EMFVLREXPRISCO"] = valor_exprisco
                        linha_final["EMFVLRDERIVATIVO"] = valor_ajuste
                        linha_final["TPFOPERADORDERIVATIVO"] = "+" if valor_ajuste > 0 else "-"
                        linha_final = linha_final.drop_duplicates(
                            subset=["EMFVLREXPRISCO", "EMFVLRDERIVATIVO", "TPFOPERADORDERIVATIVO"])
                        futuros_ajustados = futuros_ajustados.append(linha_final)

    ## Substituir Futuros
    quaid_419 = quaid_419[quaid_419["ATVCODIGO"] != "D0001"]
    quaid_419 = quaid_419.append(futuros_ajustados)

    ## Ajuste Multiplofator

    # Futuros de acoes
    if len(futuros_ajustados) > 0:
        futuros_acoes = futuros_ajustados[futuros_ajustados["FTRCODIGO"] == "AA1"]
        if len(futuros_acoes) > 0:
            futuros_acoes["FTRCODIGO"] = "JJ1"
            futuros_acoes["EMFMULTIPLOFATOR"] = 1
            for i in range(len(futuros_acoes["FTRCODIGO"])):
                futuros_acoes["TPFOPERADOR"][i] = "+" if futuros_acoes["TPFOPERADOR"][i] == "-" else "-"
            quaid_419 = quaid_419.append(futuros_acoes)

        # Futuros de dólar
        futuros_dolar = futuros_ajustados[futuros_ajustados["FTRCODIGO"] == "ME1"]
        if len(futuros_dolar) > 0:
            futuros_dolar["FTRCODIGO"] = "JJ1"
            futuros_dolar["EMFMULTIPLOFATOR"] = 1
            for i in range(len(futuros_dolar["FTRCODIGO"])):
                futuros_dolar["TPFOPERADOR"][i] = "+" if futuros_dolar["TPFOPERADOR"][i] == "-" else "-"
            quaid_419 = quaid_419.append(futuros_dolar)

    aux = pd.read_sql_query('SELECT * from projeto_inv.xml_quadro_operacoes where id_relatorio_qo=' + str(id_relatorio_qo),
                            connection)
    # aux.to_excel(end_val+'teste_qo.xlsx')

    ##################################################################################
    ## Agregação por CNPJ,Isin,Prazo
    ##################################################################################

    # Preenchecom 14 0 o cnpj
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isin(['0000000000None']), '00000000000000',
                                         quaid_419["EMFCNPJFUNDO"])
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isin([0]), '00000000000000', quaid_419["EMFCNPJFUNDO"])
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isnull(), '00000000000000', quaid_419["EMFCNPJFUNDO"])

    # Preenchecom 12 0 o isin
    quaid_419["EMFCODISIN"] = quaid_419["EMFCODISIN"].fillna('000000000000')

    # SEPARAÇÃO CAIXA O QUE NAO TEM ISIN DO QUE TEM
    semisin = quaid_419[quaid_419['tipo_produto'].isin(
        ['caixa', 'valores a pagar', 'valor das cotas a resgatar', 'valores a receber'])].copy()
    comisin = quaid_419[~quaid_419['tipo_produto'].isin(
        ['caixa', 'valores a pagar', 'valor das cotas a resgatar', 'valores a receber'])].copy()

    # COM ISIN
    x = comisin[['EMFCNPJFUNDO', 'EMFCODISIN', 'EMFPRAZOFLUXO', 'EMFVLREXPRISCO', 'EMFVLRDERIVATIVO']].groupby(
        ['EMFCNPJFUNDO', 'EMFCODISIN', 'EMFPRAZOFLUXO']).agg('sum')
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')

    teste = comisin.copy()

    del teste['EMFSEQ']
    del teste['EMFVLREXPRISCO']
    del teste['EMFVLRDERIVATIVO']

    teste = teste.drop_duplicates(subset=['EMFCNPJFUNDO', 'EMFCODISIN', 'EMFPRAZOFLUXO'], take_last=True)
    teste = pd.merge(teste, x, right_on=['EMFCNPJFUNDO', 'EMFCODISIN', 'EMFPRAZOFLUXO'],
                     left_on=['EMFCNPJFUNDO', 'EMFCODISIN', 'EMFPRAZOFLUXO'], how='left')

    # Junta com o que não tem isin
    teste = teste.append(semisin)

    # teste.to_excel(end_val+'teste.xlsx')

    quaid_419 = teste.copy()

    # Força o TPFOPERADOR
    quaid_419['TPFOPERADOR'] = np.where(quaid_419['EMFVLREXPRISCO'] >= 0, "+", "-")
    quaid_419['TPFOPERADORDERIVATIVO'] = np.where(quaid_419['EMFVLRDERIVATIVO'] >= 0, "+", "-")

    ##################################################################################
    ## Muda o fator de risco para fundos que foram abertos
    ##################################################################################

    quaid_419['ATVCODIGO'] = np.where((quaid_419['tipo_produto'].isin(['fundo'])) & (~quaid_419['FTRCODIGO'].isin(['FF1'])),
                                      'A1001', quaid_419['ATVCODIGO'])

    ##################################################################################
    ## Ajusta o TCTCODIGO
    ##################################################################################

    quaid_419['TCTCODIGO'] = np.where(quaid_419['EMFCNPJFUNDO'].isin(['00000000000000']), '01', '02')

    ##################################################################################
    ## Ajusta o TPFOPERADORDERIVATIVO
    ##################################################################################

    quaid_419['TPFOPERADORDERIVATIVO'] = np.where(
        ~quaid_419['ATVCODIGO'].isin(['D0001', 'D1001', 'D1002', 'D2001', 'D2002', 'D3001']), '0',
        quaid_419['TPFOPERADORDERIVATIVO'])

    ################################################################################
    ## Linhas adicionais
    ################################################################################

    ### Linha adicional (imóveis= IMO , fii= FII, dpvat=DPV)
    # def linha_adicional(valor, codigo, quaid_419):
    #    posicao ={}
    #    posicao["EMFSEQ"] = 1
    #    posicao["ENTCODIGO"] = ent_codigo
    #    posicao["MRFMESANO"] = dtbase
    #    posicao["QUAID"] = 419
    #    posicao["ATVCODIGO"]= "A9999" if codigo=="IMO" else "A1003"
    #    posicao["TPFOPERADOR"]= "+"
    #    posicao["FTRCODIGO"]= codigo
    #    posicao["LCRCODIGO"]= "N05"
    #    posicao["EMFCNPJFUNDO"]= 0
    #    posicao["TCTCODIGO"]= "02"
    #    posicao["TPECODIGO"]= "PR01"
    #    posicao["EMFPRAZOFLUXO"]= 1
    #    posicao["EMFVLREXPRISCO"]= valor
    #    posicao["EMFCODISIN"]= 0
    #    posicao["EMFCODCUSTODIA"]= 0
    #    posicao["EMFMULTIPLOFATOR"]= 0
    #    posicao["EMFTXCONTRATADO"]= 0
    #    posicao["EMFTXMERCADO"]= 0
    #    posicao["TPFOPERADORDERIVATIVO"]= 0
    #    posicao["EMFVLRDERIVATIVO"]= 0
    #    posicao["EMFCODGRUPO"] = 0
    #    posicao["tipo_produto"]= "Outros"
    #    quaid_419= quaid_419.append(pd.Series(posicao), ignore_index=True)
    #    return quaid_419
    #
    # quaid_419 = linha_adicional(100000, "IMO", quaid_419)

    ################################################################################
    ## Ajustes finais
    ###############################################################################

    quaid_419["EMFVLREXPRISCO"] = quaid_419["EMFVLREXPRISCO"].abs()
    quaid_419["EMFVLRDERIVATIVO"] = quaid_419["EMFVLRDERIVATIVO"].abs()

    ##JOGA FORA LINHAS COM EXPOSIÇÃO ZERADO
    quaid_419 = quaid_419[quaid_419['EMFVLREXPRISCO'] != 0].copy()

    ## Reordenar contagem de linhas
    quaid_419["EMFSEQ"] = range(1, len(quaid_419["FTRCODIGO"]) + 1, 1)

    ## Inserir data_bd
    import datetime

    quaid_419["data_bd"] = datetime.datetime.now()

    ## Inserir identificadores do relatório
    quaid_419["id_relatorio_qo"] = id_relatorio_qo
    quaid_419["tipo_relatorio"] = tipo_relatorio

    id_relatorio_quaid419 = pd.read_sql_query('SELECT max(id_relatorio_quaid419) from projeto_inv.quaid_419', connection)

    id_relatorio_quaid419 = id_relatorio_quaid419["max(id_relatorio_quaid419)"][0]
    try:
        id_relatorio_quaid419 += 1
    except:
        id_relatorio_quaid419 = 1

    quaid_419["id_relatorio_quaid419"] = id_relatorio_quaid419

    ## Substituir NaN do prazo por 1 e outros por 0
    quaid_419["EMFPRAZOFLUXO"].fillna(1, inplace=True)
    quaid_419.fillna(0, inplace=True)

    ##JOGA FORA LINHAS COM EXPOSIÇÃO ZERADO
    quaid_419 = quaid_419[quaid_419['EMFVLREXPRISCO'] != 0].copy()

    ########################################
    # Salvar informacoes no banco de dados
    ########################################

    pd.io.sql.to_sql(quaid_419, name='quaid_419', con=connection, if_exists="append", flavor='mysql', index=0,
                     chunksize=5000)

    horario_processo = datetime.datetime.today()
    aux = quaid_419[['id_relatorio_qo', 'ENTCODIGO', 'tipo_relatorio', 'id_relatorio_quaid419']].copy()
    aux['data_bd'] = horario_processo
    aux = aux.drop_duplicates()

    pd.io.sql.to_sql(aux, name='count_quadros', con=connection, if_exists="append", flavor='mysql', index=0, chunksize=5000)

    ###################################################
    # Prepara Excel para virar TXT
    ###################################################
    quadro = quaid_419.copy()
    quadro['EMFTXMERCADO'] = quadro['EMFTXMERCADO'].astype(float)
    quadro['EMFVLREXPRISCO'] = quadro['EMFVLREXPRISCO'].round(2)
    quadro['EMFVLRDERIVATIVO'] = quadro['EMFVLRDERIVATIVO'].round(2)
    quadro['EMFTXMERCADO'] = quadro['EMFTXMERCADO'].round(2)
    quadro['EMFTXCONTRATADO'] = quadro['EMFTXCONTRATADO'].round(2)

    quadro = quadro.fillna(0)

    quadro['ATVCODIGO'] = quadro['ATVCODIGO'].astype(str)
    quadro['EMFCNPJFUNDO'] = quadro['EMFCNPJFUNDO'].astype(str)
    quadro['EMFCODCUSTODIA'] = quadro['EMFCODCUSTODIA'].astype(str)
    quadro['EMFCODGRUPO'] = quadro['EMFCODGRUPO'].astype(str)
    quadro['EMFCODISIN'] = quadro['EMFCODISIN'].astype(str)
    quadro['EMFSEQ'] = quadro['EMFSEQ'].astype(str)
    quadro['EMFTXCONTRATADO'] = quadro['EMFTXCONTRATADO'].astype(str)
    quadro['EMFTXMERCADO'] = quadro['EMFTXMERCADO'].astype(str)
    quadro['ENTCODIGO'] = quadro['ENTCODIGO'].astype(str)
    quadro['FTRCODIGO'] = quadro['FTRCODIGO'].astype(str)
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].astype(str)
    quadro['MRFMESANO'] = quadro['MRFMESANO'].astype(str)
    quadro['QUAID'] = quadro['QUAID'].astype(str)
    quadro['TCTCODIGO'] = quadro['TCTCODIGO'].astype(str)
    quadro['TPFOPERADOR'] = quadro['TPFOPERADOR'].astype(str)
    quadro['TPFOPERADORDERIVATIVO'] = quadro['TPFOPERADORDERIVATIVO'].astype(str)
    quadro['EMFMULTIPLOFATOR'] = quadro['EMFMULTIPLOFATOR'].astype(str)
    quadro['EMFVLREXPRISCO'] = quadro['EMFVLREXPRISCO'].astype(str)
    quadro['EMFVLRDERIVATIVO'] = quadro['EMFVLRDERIVATIVO'].astype(str)
    quadro['TPECODIGO'] = quadro['TPECODIGO'].astype(str)
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].astype(str)
    quadro['EMFPRAZOFLUXO'] = quadro['EMFPRAZOFLUXO'].astype(str)

    # Previne contra a existencia de pontos nas strings

    quadro['ATVCODIGO'] = quadro['ATVCODIGO'].str.split('.')
    quadro['ATVCODIGO'] = quadro['ATVCODIGO'].str[0]

    quadro['EMFCNPJFUNDO'] = quadro['EMFCNPJFUNDO'].str.split('.')
    quadro['EMFCNPJFUNDO'] = quadro['EMFCNPJFUNDO'].str[0]

    quadro['EMFCODCUSTODIA'] = quadro['EMFCODCUSTODIA'].str.split('.')
    quadro['EMFCODCUSTODIA'] = quadro['EMFCODCUSTODIA'].str[0]

    quadro['EMFCODGRUPO'] = quadro['EMFCODGRUPO'].str.split('.')
    quadro['EMFCODGRUPO'] = quadro['EMFCODGRUPO'].str[0]

    quadro['EMFCODISIN'] = quadro['EMFCODISIN'].str.split('.')
    quadro['EMFCODISIN'] = quadro['EMFCODISIN'].str[0]

    quadro['EMFSEQ'] = quadro['EMFSEQ'].str.split('.')
    quadro['EMFSEQ'] = quadro['EMFSEQ'].str[0]

    quadro['ENTCODIGO'] = quadro['ENTCODIGO'].str.split('.')
    quadro['ENTCODIGO'] = quadro['ENTCODIGO'].str[0]

    quadro['FTRCODIGO'] = quadro['FTRCODIGO'].str.split('.')
    quadro['FTRCODIGO'] = quadro['FTRCODIGO'].str[0]

    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str.split('.')
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str[0]

    quadro['MRFMESANO'] = quadro['MRFMESANO'].str.split('.')
    quadro['MRFMESANO'] = quadro['MRFMESANO'].str[0]

    quadro['QUAID'] = quadro['QUAID'].str.split('.')
    quadro['QUAID'] = quadro['QUAID'].str[0]

    quadro['TCTCODIGO'] = quadro['TCTCODIGO'].str.split('.')
    quadro['TCTCODIGO'] = quadro['TCTCODIGO'].str[0]

    quadro['TPFOPERADOR'] = quadro['TPFOPERADOR'].str.split('.')
    quadro['TPFOPERADOR'] = quadro['TPFOPERADOR'].str[0]

    quadro['EMFPRAZOFLUXO'] = quadro['EMFPRAZOFLUXO'].str.split('.')
    quadro['EMFPRAZOFLUXO'] = quadro['EMFPRAZOFLUXO'].str[0]

    quadro['TPFOPERADORDERIVATIVO'] = quadro['TPFOPERADORDERIVATIVO'].str.split('.')
    quadro['TPFOPERADORDERIVATIVO'] = quadro['TPFOPERADORDERIVATIVO'].str[0]

    quadro['EMFMULTIPLOFATOR'] = quadro['EMFMULTIPLOFATOR'].str.split('.')
    quadro['EMFMULTIPLOFATOR'] = quadro['EMFMULTIPLOFATOR'].str[0]

    quadro['TPECODIGO'] = quadro['TPECODIGO'].str.split('.')
    quadro['TPECODIGO'] = quadro['TPECODIGO'].str[0]

    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str.split('.')
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str[0]

    # Preenche com a quantidade de caracteres requeridos pela SUSEP

    quadro['ATVCODIGO'] = quadro['ATVCODIGO'].str.zfill(5)
    quadro['EMFCNPJFUNDO'] = quadro['EMFCNPJFUNDO'].str.zfill(14)
    quadro['EMFCODCUSTODIA'] = quadro['EMFCODCUSTODIA'].str.zfill(12)
    quadro['EMFCODGRUPO'] = quadro['EMFCODGRUPO'].str.zfill(6)
    quadro['EMFCODISIN'] = quadro['EMFCODISIN'].str.zfill(12)
    quadro['EMFSEQ'] = quadro['EMFSEQ'].str.zfill(6)
    # quadro['EMFTXCONTRATADO']=quadro['EMFTXCONTRATADO'].str.zfill(5)
    # quadro['EMFTXMERCADO']=quadro['EMFTXMERCADO'].str.zfill(5)
    quadro['ENTCODIGO'] = quadro['ENTCODIGO'].str.zfill(5)
    quadro['FTRCODIGO'] = quadro['FTRCODIGO'].str.zfill(3)
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str.zfill(3)
    quadro['MRFMESANO'] = quadro['MRFMESANO'].str.zfill(8)
    quadro['QUAID'] = quadro['QUAID'].str.zfill(3)
    quadro['TCTCODIGO'] = quadro['TCTCODIGO'].str.zfill(2)
    quadro['TPFOPERADOR'] = quadro['TPFOPERADOR'].str.zfill(1)
    quadro['TPFOPERADORDERIVATIVO'] = quadro['TPFOPERADORDERIVATIVO'].str.zfill(1)
    quadro['EMFMULTIPLOFATOR'] = quadro['EMFMULTIPLOFATOR'].str.zfill(1)
    # quadro['EMFVLREXPRISCO']=quadro['EMFVLREXPRISCO'].str.zfill(15)
    # quadro['EMFVLRDERIVATIVO']=quadro['EMFVLRDERIVATIVO'].str.zfill(15)
    quadro['TPECODIGO'] = quadro['TPECODIGO'].str.zfill(4)
    quadro['LCRCODIGO'] = quadro['LCRCODIGO'].str.zfill(3)
    quadro['EMFPRAZOFLUXO'] = quadro['EMFPRAZOFLUXO'].str.zfill(5)

    # Reordena as colunas na ordem do quadro419 SUSEP
    quadro = quadro[
        ['EMFSEQ', 'ENTCODIGO', 'MRFMESANO', 'QUAID', 'ATVCODIGO', 'TPFOPERADOR', 'FTRCODIGO', 'LCRCODIGO', 'TCTCODIGO',
         'TPECODIGO', 'EMFPRAZOFLUXO', 'EMFVLREXPRISCO', 'EMFCNPJFUNDO', 'EMFCODISIN', 'EMFCODCUSTODIA', 'EMFMULTIPLOFATOR',
         'EMFTXCONTRATADO', 'EMFTXMERCADO', 'TPFOPERADORDERIVATIVO', 'EMFVLRDERIVATIVO', 'EMFCODGRUPO', 'tipo_produto',
         'data_bd', 'id_relatorio_qo', 'tipo_relatorio', 'id_relatorio_quaid419']].copy()

    # Troca . por , nos campos de valores
    quadro['EMFVLREXPRISCO'] = quadro['EMFVLREXPRISCO'].str.replace('.', ',')
    quadro['EMFTXMERCADO'] = quadro['EMFTXMERCADO'].str.replace('.', ',')
    quadro['EMFTXCONTRATADO'] = quadro['EMFTXCONTRATADO'].str.replace('.', ',')
    quadro['EMFVLRDERIVATIVO'] = quadro['EMFVLRDERIVATIVO'].str.replace('.', ',')

    # quadro = quadro[quadro['EMFVLREXPRISCO']!='0,0']

    quadro.to_excel(end_val + 'quadro_419_txt_' + ent_codigo + '_' + dtbase_concat + '.xlsx')

    # quaid_419.to_excel(end_val+'quaid_419_'+ent_codigo+'_'+str(id_relatorio_quaid419)+'_'+dtbase+'.xlsx')

