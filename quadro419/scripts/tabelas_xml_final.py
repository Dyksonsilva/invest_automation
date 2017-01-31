def tabelas_xml_final():

    import pandas as pd
    import pymysql as db
    import numpy as np
    import datetime
    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dt_base = datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2]))

    # Diretório de save de planilhas
    save_path_verificacao_xmls = full_path_from_database('get_output_quadro419') + 'verificacao_xmls.xlsx'

    # Diretório de dependencias
    depend_path_caracteristica_contratos = full_path_from_database('excels') + 'caracteristica_contratos.xlsx'

    ###############################################################################
    # ----Leitura do HEADER para pegar a data de referencia do relatório
    ###############################################################################

    # Informações XML
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', charset='utf8')

    query = 'SELECT * FROM projeto_inv.xml_header_org'

    xml_header = pd.read_sql(query, con=connection)

    # Seleção última carga com a data da posicao
    xml_header = xml_header[xml_header.dtposicao == dt_base]
    xml_header = xml_header[xml_header.data_bd == max(xml_header.data_bd)]

    horario_bd = datetime.datetime.today()

    xml_header = xml_header[['header_id']].copy()
    xml_header['marker'] = 1

    ###############################################################################
    # ----Preenchimento RENDA VARIÁVEL
    ###############################################################################

    # Informações XML
    query = 'SELECT * FROM projeto_inv.xml_acoes_org'

    xml_acoes = pd.read_sql(query, con=connection)

    # Seleção última carga
    xml_acoes = xml_acoes.merge(xml_header, on=['header_id'])
    xml_acoes = xml_acoes[xml_acoes.marker == 1].copy()
    xml_acoes = xml_acoes[xml_acoes.data_bd == max(xml_acoes.data_bd)]

    del xml_acoes['id_xml_acoes']
    del xml_acoes['data_bd']
    del xml_acoes['pu_regra_xml']
    del xml_acoes['mtm_regra_xml']
    del xml_acoes['data_referencia']
    del xml_acoes['marker']

    # --MTM Mercado
    query = 'SELECT * FROM projeto_inv.bovespa_cotahist'

    base_bovespa_virgem = pd.read_sql(query, con=connection)

    base_bovespa = base_bovespa_virgem[base_bovespa_virgem.data_pregao <= dt_base]

    base_bovespa = base_bovespa.sort(['codigo_isin', 'data_pregao', 'data_bd'], ascending=[True, False, False])
    base_bovespa = base_bovespa.drop_duplicates(subset=['codigo_isin'], take_last=False)
    base_bovespa = base_bovespa[['codigo_isin', 'preco_medio', 'data_pregao']]
    base_bovespa = base_bovespa.rename(
        columns={'codigo_isin': 'isin', 'preco_medio': 'pu_regra_xml', 'data_pregao': 'data_referencia'})

    xml_acoes = xml_acoes.merge(base_bovespa, on=['isin'], how='left')

    xml_acoes['mtm_regra_xml'] = xml_acoes['pu_regra_xml'] * (xml_acoes['qtdisponivel'] + xml_acoes['qtgarantia'])

    xml_acoes['data_bd'] = horario_bd

    # Carrega na base

    pd.io.sql.to_sql(xml_acoes, name='xml_acoes', con=connection, if_exists="append", flavor='mysql', index=0)

    ###############################################################################
    # ----Preenchimento Futuros
    ###############################################################################

    # ----TABELA XML
    query = 'select * from projeto_inv.xml_futuros_org'

    xml_futuros = pd.read_sql(query, con=connection)

    xml_futuros = xml_futuros.merge(xml_header, on=['header_id'])
    xml_futuros = xml_futuros[xml_futuros.marker == 1].copy()
    xml_futuros = xml_futuros[xml_futuros.data_bd == max(xml_futuros.data_bd)]

    del xml_futuros['id_xml_futuros']
    del xml_futuros['pu_regra_xml']
    del xml_futuros['mtm_regra_xml']
    del xml_futuros['data_referencia']
    del xml_futuros['marker']

    # Criação da coluna ref
    xml_futuros['ref'] = xml_futuros['ativo'] + xml_futuros['serie']

    # Retira coluna de data da carga
    del xml_futuros['data_bd']

    # -----\TABELA BMF
    query = 'SELECT * FROM projeto_inv.bmf_ajustes_pregao'
    bmf_ajustes_pregao = pd.read_sql(query, con=connection)

    bmf_ajustes_pregao = bmf_ajustes_pregao.sort(['mercadoria', 'data_referencia', 'data_bd'], ascending=[True, True, True])

    # Seleciona data de referência igual ou menor à do relatório
    bmf_ajustes_pregao = bmf_ajustes_pregao[bmf_ajustes_pregao.data_referencia <= dt_base]

    # Seleciona a última data para cada contrato
    bmf_ajustes_pregao = bmf_ajustes_pregao.drop_duplicates(subset=['mercadoria', 'vencimento'], take_last=True)

    # Seleciona a coluna ref - XXX referencia à mercadoria
    bmf_ajustes_pregao['ref'] = bmf_ajustes_pregao['mercadoria'].str[0:3]

    # Retira coluna de data da carga
    del bmf_ajustes_pregao['data_bd']

    # ----TABELA TAMANHO DO CONTRATO
    tamanho_contrato = pd.read_excel(depend_path_caracteristica_contratos, header=0, skiprows=1)
    tamanho_contrato.columns = ['ref', 'moeda', 'tamanho']

    tamanho_contrato['ref'] = tamanho_contrato['ref'].str.replace(' ', '')

    base_input = bmf_ajustes_pregao.merge(tamanho_contrato, on=['ref'], how='left')
    base_input = base_input.where(base_input.notnull(), None)

    base_input['ref'] = base_input['ref'] + base_input['vencimento']

    # ----UNIÃO DAS TABELAS

    xml_futuros = xml_futuros.merge(base_input, on=['ref'], how='left')
    xml_futuros = xml_futuros.sort(['ref'])

    # Criação da coluna pu_mercado - NÃO CONSIDERA O TAMANHO DO CONTRATO
    xml_futuros = xml_futuros.rename(columns={'preco_ajuste_atual': 'pu_regra_xml'})

    # Criação da coluna mtm_mercado
    xml_futuros['mtm_regra_xml'] = xml_futuros['pu_regra_xml'] * xml_futuros['quantidade'] * xml_futuros['tamanho']

    # Retira colunas não necessárias
    del xml_futuros['ref']
    del xml_futuros['id_bmf_ajustes_pregao']
    del xml_futuros['mercadoria']
    del xml_futuros['vencimento']
    del xml_futuros['preco_ajuste_anterior']
    del xml_futuros['variacao']
    del xml_futuros['valor_ajuste_por_contrato']
    del xml_futuros['moeda']
    del xml_futuros['tamanho']

    # Cria coluna com a data da carga
    xml_futuros['data_bd'] = datetime.datetime.today()

    # Carrega na base

    pd.io.sql.to_sql(xml_futuros, name='xml_futuros', con=connection, if_exists="append", flavor='mysql', index=0)

    ###############################################################################
    # ----Preenchimento Cotas
    ###############################################################################

    # ----TABELA XML
    query = 'SELECT * FROM projeto_inv.xml_cotas_org'

    xml_cotas = pd.read_sql(query, con=connection)

    xml_cotas = xml_cotas.merge(xml_header, on=['header_id'])
    xml_cotas = xml_cotas[xml_cotas.marker == 1].copy()
    xml_cotas = xml_cotas[xml_cotas.data_bd == max(xml_cotas.data_bd)]

    xml_cotas['data_referencia'] = dt_base

    del xml_cotas['id_xml_cotas']
    del xml_cotas['pu_regra_xml']
    del xml_cotas['data_bd']

    # ----TABELA CVM_COTAS
    query = 'SELECT * FROM projeto_inv.cvm_cotas'

    cvm_cotas = pd.read_sql(query, con=connection)

    cvm_cotas = cvm_cotas.sort(['cnpj_fundo', 'dt_ref', 'data_bd'], ascending=[True, True, False])
    cvm_cotas = cvm_cotas.drop_duplicates(['cnpj_fundo', 'dt_ref'], take_last=False)

    # ----TABELA FIDCs
    query = 'SELECT * FROM projeto_inv.fidc_cotas'

    fidc_cotas = pd.read_sql(query, con=connection)

    fidc_cotas = fidc_cotas.sort(['codigo_isin', 'dt_ref', 'data_bd'], ascending=[True, True, False])
    fidc_cotas = fidc_cotas.drop_duplicates(['codigo_isin', 'dt_ref'], take_last=False)

    # ----Preenchimento com as informações - CVM_COTAS

    # Seleção das colunas necessárias
    cvm_cotas = cvm_cotas[['cnpj_fundo', 'dt_ref', 'quota']].copy()

    # Renomeação da chave cnpj_fundos
    cvm_cotas = cvm_cotas.rename(columns={'cnpj_fundo': 'cnpjfundo', 'dt_ref': 'data_referencia'})

    # União
    xml_cotas = xml_cotas.merge(cvm_cotas, on=['cnpjfundo', 'data_referencia'], how='left')

    # ----Preenchimento com as informações - CVM_FIDC

    # Seleção das colunas necessárias
    fidc_cotas = fidc_cotas[['codigo_isin', 'dt_ref', 'cota']].copy()

    # Renomeação da chave cnpj_fundos
    fidc_cotas = fidc_cotas.rename(columns={'codigo_isin': 'isin', 'dt_ref': 'data_referencia'})

    # União
    xml_cotas = xml_cotas.merge(fidc_cotas, on=['isin', 'data_referencia'], how='left')

    # ----PREENCHIMENTO XML

    # Preenchimento do PU
    xml_cotas['pu_regra_xml'] = np.where(xml_cotas['quota'].notnull(), xml_cotas['quota'], xml_cotas['cota'])
    xml_cotas['pu_regra_xml'] = np.where(xml_cotas['pu_regra_xml'].isnull(), xml_cotas['puposicao'],
                                         xml_cotas['pu_regra_xml'])

    # Cálculo do MTM
    xml_cotas['mtm_regra_xml'] = xml_cotas['pu_regra_xml'] * (xml_cotas['qtdisponivel'] + xml_cotas['qtgarantia'])

    # ----Carregamento na base
    xml_cotas['data_bd'] = horario_bd

    del xml_cotas['marker']
    del xml_cotas['cota']
    del xml_cotas['quota']

    pd.io.sql.to_sql(xml_cotas, name='xml_cotas', con=connection, if_exists="append", flavor='mysql', index=0)

    ###############################################################################
    # ----Outras tabelas
    ###############################################################################

    lista = ['outrasdespesas',
             'caixa',
             'corretagem',
             'despesas',
             'partplanprev',
             'provisao',
             'ima_geral',
             'ima_imab',
             'ima_imac',
             'ima_imas',
             'ima_irfm',
             'opcoesacoes',
             'opcoesderiv',
             'opcoesflx',
             'swap',
             'termorv',
             'termorf',
             'header']

    horario_bd = datetime.datetime.today()
    for i in range(len(lista)):
        x = 'select * from projeto_inv.xml_' + lista[i] + '_org'
        tabela = pd.read_sql(x, con=connection)
        if (len(tabela) != 0):
            print('xml_' + lista[i])
            tabela = tabela.merge(xml_header, on=['header_id'])
            tabela = tabela[tabela.marker == 1].copy()
            if 'pu_regra_xml' in tabela.columns:
                tabela['pu_regra_xml'] = tabela['puposicao']
            if 'mtm_regra_xml' in tabela.columns:
                tabela['mtm_regra_xml'] = tabela['puposicao']
            if 'mtm_ativo_regra_xml' in tabela.columns:
                tabela['mtm_ativo_regra_xml'] = tabela['vlmercadoativo']
            if 'mtm_passivo_regra_xml' in tabela.columns:
                tabela['mtm_passivo_regra_xml'] = tabela['vlmercadopassivo']
            if ('data_referencia' in tabela.columns) & (len(tabela) != 0):
                tabela['data_referencia'] = dt_base
            tabela['data_bd'] = horario_bd
            id_xml = 'id_xml_' + lista[i]
            if id_xml in tabela.columns:
                del tabela[id_xml]
                del tabela['marker']
            pd.io.sql.to_sql(tabela, name='xml_' + lista[i], con=connection, if_exists="append", flavor='mysql', index=0)

    writer = ExcelWriter(save_path_verificacao_xmls)

    lista = ['outrasdespesas',
             'caixa',
             'corretagem',
             'despesas',
             'partplanprev',
             'provisao',
             'ima_geral',
             'ima_imab',
             'ima_imac',
             'ima_imas',
             'ima_irfm',
             'opcoesacoes',
             'opcoesderiv',
             'opcoesflx',
             'swap',
             'termorv',
             'termorf',
             'header',
             'debenture',
             'titpublico',
             'titprivado',
             'acoes',
             'cotas',
             'futuros']

    horario_bd = datetime.datetime.today()
    for i in range(len(lista)):
        x = 'select * from projeto_inv.xml_' + lista[i]
        tabela = pd.read_sql(x, con=connection)
        if len(tabela) != 0:
            tabela = tabela[tabela.data_bd == max(tabela.data_bd)].copy()
        tabela.to_excel(writer, lista[i])

    writer.save()

    connection.close()