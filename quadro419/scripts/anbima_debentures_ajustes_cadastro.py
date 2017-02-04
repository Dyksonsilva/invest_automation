def anbima_debentures_ajustes_cadastro():
    import pandas as pd
    import datetime
    import pymysql as db
    import numpy as np
    import logging

    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    # Definições de paths utilizados no script
    logger = logging.getLogger(__name__)
    xlsx_path_controle_debentures = full_path_from_database('get_output_quadro419') + 'controle_debentures.xlsx'
    xlsx_path_lista_debentures = full_path_from_database('excels') + 'LISTA_debenture_fluxo_manual.xlsx'
    xlsx_path_caracteristicas_debentures = full_path_from_database('excels') + 'caracteristicas_debenture.xlsx'
    xlsx_path_eventos_financeiros = full_path_from_database('excels') + 'eventos_financeiros.xlsx'

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    #dtbase = ['2016', '11', '30']
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]
    horario_bd = datetime.datetime.now()

    #Leitura e tratamento das tabelas
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv'
, use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * FROM projeto_inv.anbima_debentures_caracteristicas'
    debenture_anbima = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    logger.info("Tratando dados")
    debenture_anbima = debenture_anbima.sort(['isin', 'data_bd'], ascending=[True, False])
    debenture_anbima = debenture_anbima.drop_duplicates(subset=['isin'], take_last=True)

    # Criação do flag 'O' - existe nas caracteristicas anbima
    debenture_anbima['flag'] = 'O'

    # ----Tratamento da tabela anbima

    # Substitui nan por None
    debenture_anbima = debenture_anbima.where((pd.notnull(debenture_anbima)), None)

    # Transforma '-' em None
    debenture_anbima = debenture_anbima.replace({'-': None}, regex=False)

    # Limpa o espaços antes e depois das strings da última linha
    n = len(debenture_anbima)
    if debenture_anbima['data_de_emissao'].iloc[n - 1] is not None:
        debenture_anbima['data_de_emissao'].iloc[n - 1] = debenture_anbima['data_de_emissao'].iloc[n - 1].strip()
    if debenture_anbima['data_de_vencimento'].iloc[n - 1] is not None:
        debenture_anbima['data_de_vencimento'].iloc[n - 1] = debenture_anbima['data_de_vencimento'].iloc[n - 1].strip()
    if debenture_anbima['data_do_inicio_da_rentabilidade'].iloc[n - 1] is not None:
        debenture_anbima['data_do_inicio_da_rentabilidade'].iloc[n - 1] = \
            debenture_anbima['data_do_inicio_da_rentabilidade'].iloc[n - 1].strip()
    if debenture_anbima['amortizacao_carencia'].iloc[n - 1] is not None:
        debenture_anbima['amortizacao_carencia'].iloc[n - 1] = debenture_anbima['amortizacao_carencia'].iloc[
            n - 1].strip()
    if debenture_anbima['juros_criterio_novo_carencia'].iloc[n - 1] is not None:
        debenture_anbima['juros_criterio_novo_carencia'].iloc[n - 1] = \
            debenture_anbima['juros_criterio_novo_carencia'].iloc[n - 1].strip()

    # Verificação do que dá para fazer com os Não Padrão - SND
    deb_np = debenture_anbima[debenture_anbima.criterio_de_calculo == 'Não Padrão - SND']

    # ----Atualização das informações juros_criterio_novo para papéis não preeenchidos no Padrão - SND
    # Taxa de juros
    deb_np['juros_criterio_antigo_do_snd'] = deb_np['juros_criterio_antigo_do_snd'].str.split('%')
    deb_np['juros_criterio_novo_taxa'] = deb_np['juros_criterio_antigo_do_snd'].str[0]
    # Juros_dc_du
    deb_np['juros_criterio_novo_prazo'] = deb_np['juros_criterio_antigo_do_snd'].str[1]
    deb_np['juros_criterio_novo_prazo'] = deb_np['juros_criterio_novo_prazo'].str.split(' ')
    deb_np['juros_criterio_novo_prazo'] = deb_np['juros_criterio_novo_prazo'].str[2]
    deb_np['juros_criterio_novo_prazo'] = deb_np['juros_criterio_novo_prazo'].str.replace(' ', '')
    # Data primeiro pagamento
    deb_np['juros_criterio_novo_carencia'] = deb_np['amortizacao_carencia']
    # Muda o critério
    deb_np['criterio_de_calculo'] = 'Padrão - SND'
    deb_np['juros_criterio_antigo_do_snd'] = None
    # Cria o flag de criação
    deb_np['flag'] = 'CP'

    # Une as duas tabelas
    debenture_anbima = debenture_anbima[debenture_anbima.criterio_de_calculo != 'Não Padrão - SND']
    debenture_anbima = debenture_anbima.append(deb_np)

    # Deleta observações com campo "Data de Vencimento" = "Indeterminado"
    debenture_anbima = debenture_anbima[debenture_anbima.data_de_vencimento != 'Indeterminado']

    # Preenche com a data de emissão onde a data de inicio de rentabilidade está vazio
    debenture_anbima['data_do_inicio_da_rentabilidade'][debenture_anbima.data_do_inicio_da_rentabilidade.isnull()] = \
        debenture_anbima['data_de_emissao'][debenture_anbima.data_do_inicio_da_rentabilidade.isnull()]

    # Deleta observações com campo "Valor Nominal de Emissão" em unidade diferente do real
    debenture_anbima = debenture_anbima[debenture_anbima.unidade_monetaria_1 != 'NCz$']
    debenture_anbima = debenture_anbima[debenture_anbima.unidade_monetaria_1 != 'CR$']
    debenture_anbima = debenture_anbima[debenture_anbima.unidade_monetaria_1 != 'Cr$']

    # Alteração de formato das datas
    df = debenture_anbima[
        ['isin', 'data_de_emissao', 'data_de_vencimento', 'data_do_inicio_da_rentabilidade', 'amortizacao_carencia',
         'juros_criterio_novo_carencia']].copy()

    col_list = df.columns.values
    col_list = np.delete(col_list, 0)
    for i in col_list:
        df1 = df[['isin', i]][df[i].notnull()].copy()
        df1['data_str'] = df1[i].str.split('/')

        df1['ano'] = df1['data_str'].str[2]
        df1['ano'] = df1['ano'].astype(str)

        df1['mes'] = df1['data_str'].str[1]
        df1['mes'] = df1['mes'].astype(str)
        df1['mes'] = df1['mes'].str.zfill(2)

        df1['dia'] = df1['data_str'].str[0]
        df1['dia'] = df1['dia'].astype(str)
        df1['dia'] = df1['dia'].str.zfill(2)

        del df1[i]
        del df1['data_str']

        df1[i] = df1['ano'] + '-' + df1['mes'] + '-' + df1['dia']
        df1[i] = pd.to_datetime(df1[i]).dt.date

        del df1['ano']
        del df1['mes']
        del df1['dia']
        del df[i]

        df = df.merge(df1, on=['isin'], how='left')

    df = df.where((pd.notnull(df)), None)

    del debenture_anbima['data_de_emissao']
    del debenture_anbima['data_de_vencimento']
    del debenture_anbima['data_do_inicio_da_rentabilidade']
    del debenture_anbima['amortizacao_carencia']
    del debenture_anbima['juros_criterio_novo_carencia']

    debenture_anbima = debenture_anbima.merge(df, on=['isin'])

    # Deleta colunas não necessárias
    del debenture_anbima['data_bd']
    del debenture_anbima['id_anbima_debentures_caracteristicas']

    # ----Leitura da tabela xml

    query = 'SELECT * FROM projeto_inv.xml_debenture_org'
    debenture_xml = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    # Seleciona última carga com dtposicao = data_base
    debenture_xml['dtrel'] = debenture_xml['id_papel'].str.split('_')
    debenture_xml['dtrel'] = debenture_xml['dtrel'].str[0]
    debenture_xml = debenture_xml[debenture_xml.dtrel == dtbase_concat].copy()
    debenture_xml = debenture_xml[debenture_xml.data_bd == max(debenture_xml.data_bd)]

    query = 'SELECT * FROM projeto_inv.xml_header_org '
    header_xml = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    # Seleciona última carga com dtposicao = data_base
    header_xml = header_xml[
        header_xml.dtposicao == datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2]))].copy()
    header_xml = header_xml[header_xml.data_bd == max(header_xml.data_bd)]

    # ----Tratamento da tabela xml

    # Seleção das colunas necessárias
    debenture_xml = debenture_xml[['isin',
                                   'coddeb',
                                   'dtemissao',
                                   'dtoperacao',
                                   'dtvencimento',
                                   'puemissao',
                                   'coupom',
                                   'indexador',
                                   'percindex',
                                   'id_papel']]

    # Renomeação de colunas
    debenture_xml = debenture_xml.rename(columns={'indexador': 'indice_xml', 'coddeb': 'codigo_do_ativo_xml'})
    debenture_xml['codigo_do_ativo_xml'] = debenture_xml['codigo_do_ativo_xml'].str.replace(' ', '')

    # ----Criação da tabela xml + anbima

    # Constrói base xml+anbima
    debenture = debenture_xml.merge(debenture_anbima, on=['isin'], how='left')

    # Arruma os NaN
    debenture = debenture.where((pd.notnull(debenture)), None)

    # Transforma isin-> codigo_isin
    debenture = debenture.rename(columns={'isin': 'codigo_isin'})

    #Manipulação das informações cadastrais

    # Criação de excel com a lista de manipulações
    writer = ExcelWriter(xlsx_path_controle_debentures)

    # Relatório de situação isin
    relatorio = debenture[['codigo_isin', 'situacao']].copy()
    relatorio.to_excel(writer, 'situacao_isin')
    logger.info("Arquivos salvos com sucesso")

    # ----Criação papéis AS BULLET
    debenture_criacao = debenture[debenture.flag.isnull()].copy()
    debenture_criacao['codigo_do_ativo'] = debenture_criacao['codigo_do_ativo_xml']
    debenture_criacao['data_de_emissao'] = debenture_criacao['dtemissao']
    debenture_criacao['data_de_vencimento'] = debenture_criacao['dtvencimento']
    debenture_criacao['data_do_inicio_da_rentabilidade'] = debenture_criacao['dtemissao']
    debenture_criacao['amortizacao_carencia'] = debenture_criacao['dtvencimento']
    debenture_criacao['juros_criterio_novo_carencia'] = debenture_criacao['dtvencimento']
    debenture_criacao['juros_criterio_novo_taxa'] = debenture_criacao['coupom']
    debenture_criacao['indice'] = debenture_criacao['indice_xml']
    debenture_criacao['percentual_multiplicador_rentabilidade'] = debenture_criacao['percindex']
    debenture_criacao['valor_nominal_na_emissao'] = debenture_criacao['puemissao']
    debenture_criacao['flag'] = 'C0'

    # União das tabelas
    debenture = debenture[debenture.flag.notnull()]
    debenture = debenture.append(debenture_criacao)

    # Salva as alterações num excel
    alteracao = debenture[debenture.flag == 'C0'].copy()
    alteracao.to_excel(writer, 'criados_0')
    logger.info("Arquivos salvos com sucesso")

    # ----Alteração dos papéis na carteira fora do Padrão - SND na base da anbima e tem padrão de pagamento uniforme

    # Salva os papéis na carteira que foram alterados de Não Padrão - SND -> Padrão SND
    alteracao = debenture[debenture.flag == 'CP'].copy()
    alteracao.to_excel(writer, 'criados_p')

    # Verificação se tem fluxo manual
    amt_plan = pd.read_excel(xlsx_path_lista_debentures, header=0)
    amt_plan = amt_plan.rename(columns={'flag': 'flag1'})

    debenture = debenture.merge(amt_plan, on=['codigo_isin'], how='left')

    # Retira os papéis que tem fluxo manual e seleciona somente os papéis que foram alterados em CP
    debenture_criacao = debenture[(debenture.flag1.isnull()) & (debenture.flag == 'CP')].copy()
    del debenture_criacao['flag1']

    # Seleciona papéis tem padrão de amortizacao uniforme
    a7 = 'Percentual fixo sobre o valor nominal atualizado em períodos uniformes'
    a8 = 'Percentual fixo sobre o valor nominal de emissão em períodos uniformes'

    debenture_criacao = debenture_criacao[debenture_criacao['tipo_de_amortizacao'].isin([a7, a8])].copy()

    # codigo_isin BRCTAXDBS062
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']), 3,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']),
                                                                 datetime.date(2017, 3, 15),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']), 1.25,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']), 3,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']),
                                                         datetime.date(2018, 3, 15),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['data_do_inicio_da_rentabilidade'] = np.where(
        debenture_criacao.codigo_isin.isin(['BRCTAXDBS062']),
        datetime.date(2015, 8, 30),
        debenture_criacao['data_do_inicio_da_rentabilidade'])

    # codigo_isin BRENBRDBS038
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']), 1.74,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']),
                                                                 datetime.date(2015, 3, 15),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRENBRDBS038']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])

    # codigo_isin BRMRSADBS073
    debenture_criacao['juros_criterio_novo_prazo'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']), 252,
                                                              debenture_criacao['juros_criterio_novo_prazo'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']), 2.9,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']),
                                                                 datetime.date(2014, 6, 10),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRMRSADBS073']),
                                                         datetime.date(2017, 12, 10),
                                                         debenture_criacao['amortizacao_carencia'])

    # codigo_isin BRGFSADBS0A6
    debenture_criacao['juros_criterio_novo_prazo'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']), 252,
                                                              debenture_criacao['juros_criterio_novo_prazo'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']), 8.22,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']),
                                                                 datetime.date(2016, 1, 20),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']), 12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRGFSADBS0A6']),
                                                         datetime.date(2018, 1, 20),
                                                         debenture_criacao['amortizacao_carencia'])

    # codigo_isin BRELPLDBS0F9
    debenture_criacao['juros_criterio_novo_prazo'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']), 252,
                                                              debenture_criacao['juros_criterio_novo_prazo'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']), 1.12,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']),
                                                                 datetime.date(2006, 6, 20),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRELPLDBS0F9']),
                                                         datetime.date(2015, 8, 20),
                                                         debenture_criacao['amortizacao_carencia'])

    # codigo_isin BRENGIDBS044
    debenture_criacao['juros_criterio_novo_prazo'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']), 252,
                                                              debenture_criacao['juros_criterio_novo_prazo'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']), 6.15,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']),
                                                                 datetime.date(2013, 7, 15),
                                                                 debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']), 12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']),
                                                                'MES',
                                                                debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao.codigo_isin.isin(['BRENGIDBS044']),
                                                         datetime.date(2018, 7, 15),
                                                         debenture_criacao['amortizacao_carencia'])

    debenture_criacao['flag'] = 'C1'

    debenture['flag'] = np.where((debenture['flag1'].isnull()) & (debenture['flag'] == 'CP'), 'C1', debenture['flag'])
    debenture['flag'] = np.where((debenture['flag1'].notnull()) & (debenture['flag'] == 'CP'), debenture['flag1'],
                                 debenture['flag'])

    del debenture['flag1']
    debenture = debenture[debenture.flag != 'C1'].copy()
    debenture = debenture.append(debenture_criacao)

    # Salva as alterações num excel
    alteracao = debenture[debenture.flag == 'C1'].copy()
    alteracao.to_excel(writer, 'criados_1')

    # ----Papéis com fluxo de amortização/juros não uniforme
    # Lista de papéis com plano de amortização não uniforme
    a1 = 'Percentual fixo sobre o valor nominal atualizado em períodos não uniformes'
    a2 = 'Percentual variável sobre o valor nominal atualizado em períodos não uniformes'
    a3 = 'Percentual variável sobre o valor nominal atualizado em períodos uniformes'
    a4 = 'Percentual fixo sobre o valor nominal de emissão em períodos não uniformes'
    a5 = 'Percentual variável sobre o valor nominal de emissão em períodos não uniformes'
    a6 = 'Percentual variável sobre o valor nominal de emissão em períodos uniformes'

    amt_nu = debenture[['id_papel', 'codigo_isin', 'registro_cvm_da_emissao']][
        debenture.tipo_de_amortizacao.isin([a1, a2, a3, a4, a5, a6])].copy()

    # Verificação de fluxo manual
    amt_plan = pd.read_excel(xlsx_path_lista_debentures, header=0)
    logger.info("Leitura do banco de dados executada com sucesso")

    amt_nu = amt_nu.merge(amt_plan, on=['codigo_isin'], how='left')

    # Salva os papéis com plano de amortização na uniforme num excel - os papéis que tem flag=FM tem fluxo manual inserido no final do programa de fluxo
    amt_nu.to_excel(writer, 'amt_plan')

    # Cria flag de criação para os papéis que não tem fluxo manual listado no excel LISTA_debenture_fluxo_manual.xlsx
    amt_nu['flag'] = np.where(amt_nu['flag'].isnull(), 'C2', amt_nu['flag'])

    amt_nu = amt_nu.rename(columns={'flag': 'flag1'})
    del amt_nu['registro_cvm_da_emissao']

    debenture = debenture.merge(amt_nu, on=['id_papel', 'codigo_isin'], how='left')
    debenture['flag'] = np.where(debenture['flag1'].notnull(), debenture['flag1'], debenture['flag'])
    del debenture['flag1']

    # ----Criação dos papéis que são Dispensa ICVM 476 e que tem padrão de pagamento não uniforme
    debenture_criacao = debenture[debenture.flag == 'C2'].copy()

    carac = pd.read_excel(xlsx_path_caracteristicas_debentures, header=0)

    debenture_criacao = pd.merge(debenture_criacao, carac, left_on=['codigo_isin'], right_on=['codigo_isin'],
                                 how='left')

    # tipo_de_amortizacao OLD
    a1 = 'Percentual fixo sobre o valor nominal atualizado em períodos não uniformes'
    a2 = 'Percentual variável sobre o valor nominal atualizado em períodos não uniformes'
    a3 = 'Percentual variável sobre o valor nominal atualizado em períodos uniformes'
    a4 = 'Percentual fixo sobre o valor nominal de emissão em períodos não uniformes'
    a5 = 'Percentual variável sobre o valor nominal de emissão em períodos não uniformes'
    a6 = 'Percentual variável sobre o valor nominal de emissão em períodos uniformes'
    # tipo_de_amortizacao NEW
    a7 = 'Percentual fixo sobre o valor nominal atualizado em períodos uniformes'
    a8 = 'Percentual fixo sobre o valor nominal de emissão em períodos uniformes'

    debenture_criacao['indice'] = debenture_criacao['indexador1']
    debenture_criacao['percentual_multiplicador_rentabilidade'] = debenture_criacao[
        'percentual_multiplicador_rentabilidade1']
    debenture_criacao['juros_criterio_novo_taxa'] = debenture_criacao['juros_criterio_novo_taxa1']
    debenture_criacao['juros_criterio_novo_cada'] = debenture_criacao['juros_criterio_novo_cada1']
    debenture_criacao['juros_criterio_novo_carencia'] = debenture_criacao['juros_criterio_novo_carencia1'].dt.date
    debenture_criacao['juros_criterio_novo_unidade'] = 'MES'
    debenture_criacao['juros_criterio_novo_prazo'] = '252'
    debenture_criacao['amortizacao_carencia'] = debenture_criacao['amortizacao_carencia1'].dt.date
    debenture_criacao['amortizacao_cada'] = debenture_criacao['amortizacao_cada1']
    debenture_criacao['amortizacao_unidade'] = 'MES'
    debenture_criacao['tipo_de_amortizacao'] = np.where(debenture_criacao['tipo_de_amortizacao'].isin([a1, a2, a3]), a7,
                                                        debenture_criacao['tipo_de_amortizacao'])
    debenture_criacao['tipo_de_amortizacao'] = np.where(debenture_criacao['tipo_de_amortizacao'].isin([a4, a5, a6]), a8,
                                                        debenture_criacao['tipo_de_amortizacao'])

    del debenture_criacao['indexador1']
    del debenture_criacao['percentual_multiplicador_rentabilidade1']
    del debenture_criacao['juros_criterio_novo_taxa1']
    del debenture_criacao['juros_criterio_novo_cada1']
    del debenture_criacao['juros_criterio_novo_carencia1']
    del debenture_criacao['amortizacao_carencia1']
    del debenture_criacao['amortizacao_cada1']

    debenture = debenture[debenture.flag != 'C2']
    debenture = debenture.append(debenture_criacao)

    # ----Resolve papéis que não tem tipo_de_amortizacao
    debenture_criacao = debenture[(debenture.tipo_de_amortizacao.isnull())].copy()

    # Puxa informação da planilha com eventos financeiros para verificação se o papel de fato é bullet no pagamento de amortização
    eventos_financeiros = pd.read_excel(xlsx_path_eventos_financeiros, header=0)
    eventos_financeiros = eventos_financeiros[['Data do Evento', 'Ativo', 'Evento', 'Taxa/Percentual']].copy()
    eventos_financeiros = eventos_financeiros.rename(columns={'Ativo': 'codigo_do_ativo'})

    debenture_criacao1 = debenture.merge(eventos_financeiros, on=['codigo_do_ativo'], how='left')

    # Arquivo de verificação
    # debenture_criacao1[['id_papel','codigo_isin','data_de_vencimento','juros_criterio_novo_carencia','amortizacao_carencia','juros_criterio_novo_cada','Data do Evento','Evento','Taxa/Percentual']].to_excel(base_dir+'/DEBENTURE_verificacao_evento_financeiro.xlsx')
    del debenture_criacao1

    # Quando é DI, automaticamente coloca pagamento de VNE
    a7 = 'Percentual fixo sobre o valor nominal atualizado em períodos uniformes'
    a8 = 'Percentual fixo sobre o valor nominal de emissão em períodos uniformes'

    # DI -> VNE, demais -> VNA
    debenture_criacao['tipo_de_amortizacao'] = np.where(debenture_criacao['indice'].isin(['DI', 'DI1']), a8, a7)

    # Alteração default - assume bullet quando não há data do primeiro pagamento
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['amortizacao_carencia'].isnull(),
                                                         debenture_criacao['data_de_vencimento'],
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['juros_criterio_novo_carencia'].isnull(),
        debenture_criacao['data_de_vencimento'],
        debenture_criacao['amortizacao_carencia'])

    #ALTERAÇÕES ESPECÍFICAS DA RODADA
    # Alteração default - todos pagam mensalmente - VERIFICAR SEMPRE ANTES DE CONTINUAR COM ESTE COMANDO
    debenture_criacao['juros_criterio_novo_unidade'] = debenture_criacao['juros_criterio_novo_unidade'].fillna('MES')

    # Alteração específica isin = BRALSCDBS028
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']),
                                                         datetime.date(2023, 1, 31),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']), 50,
                                                     debenture_criacao['amortizacao_taxa'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']),
        datetime.date(2015, 1, 31),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRALSCDBS028']),
                                                             1000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRGETIDBS041
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']),
                                                         datetime.date(2017, 5, 15),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']),
                                                     33.3333333,
                                                     debenture_criacao['amortizacao_taxa'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']),
        datetime.date(2013, 11, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS041']),
                                                             10000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRGETIDBS058
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']),
                                                         datetime.date(2018, 3, 20),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']), 12,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']),
                                                     33.3333333,
                                                     debenture_criacao['amortizacao_taxa'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']),
        datetime.date(2014, 9, 20),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS058']),
                                                             10000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRGETIDBS066
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS066']),
                                                             1.9,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS066']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRGETIDBS066']),
        datetime.date(2016, 6, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRGETIDBS066']),
                                                             1000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRBDLSDBS008
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBDLSDBS008']),
                                                             None,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBDLSDBS008']),
                                                             None,
                                                             debenture_criacao['juros_criterio_novo_taxa'])

    # Alteração específica isin = BRBNDPDBS0B9
    debenture_criacao['juros_criterio_novo_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBNDPDBS0B9']),
                                                             0.55,
                                                             debenture_criacao['juros_criterio_novo_taxa'])
    debenture_criacao['percentual_multiplicador_rentabilidade'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRBNDPDBS0B9']), 112.99,
        debenture_criacao['percentual_multiplicador_rentabilidade'])
    debenture_criacao['juros_criterio_novo_prazo'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBNDPDBS0B9']),
                                                              360,
                                                              debenture_criacao['juros_criterio_novo_prazo'])
    debenture_criacao['juros_criterio_novo_tipo'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBNDPDBS0B9']),
                                                             'Exponencial',
                                                             debenture_criacao['juros_criterio_novo_tipo'])
    debenture_criacao['tipo_de_amortizacao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBNDPDBS0B9']), a8,
                                                        debenture_criacao['tipo_de_amortizacao'])

    # Alteração específica isin = BRLAMEDBS043
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS043']),
                                                         datetime.date(2018, 1, 26),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRLAMEDBS043']),
        datetime.date(2013, 1, 26),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS043']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS043']),
                                                             10000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRLAMEDBS050
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS050']),
                                                         datetime.date(2018, 1, 26),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRLAMEDBS050']),
        datetime.date(2013, 1, 26),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS050']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLAMEDBS050']),
                                                             10000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    # Alteração específica isin = BRCMGDDBS025
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCMGDDBS025']),
        datetime.date(2014, 2, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGDDBS025']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGDDBS025']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BROIBRDBS052
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BROIBRDBS052']),
        datetime.date(2014, 3, 28),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BROIBRDBS052']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BROIBRDBS052']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRCMGTDBS047
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCMGTDBS047']),
        datetime.date(2013, 2, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGTDBS047']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGTDBS047']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRCMGTDBS070
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCMGTDBS070']),
        datetime.date(2014, 12, 23),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGTDBS070']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCMGTDBS070']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRJSLGDBS079
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRJSLGDBS079']),
        datetime.date(2014, 1, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRJSLGDBS079']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRJSLGDBS079']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRVIVTDBS044
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRVIVTDBS044']),
        datetime.date(2013, 3, 19),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRVIVTDBS044']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRVIVTDBS044']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRVIVTDBS051
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRVIVTDBS051']),
        datetime.date(2013, 10, 25),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRVIVTDBS051']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRVIVTDBS051']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRTAEEDBS050
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRTAEEDBS050']),
        datetime.date(2013, 10, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRTAEEDBS050']),
                                                             12,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRTAEEDBS050']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRRESADBS013
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRRESADBS013']),
        datetime.date(2014, 4, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRRESADBS013']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRRESADBS013']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRMYPKDBS058
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRMYPKDBS058']),
        datetime.date(2014, 1, 10),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRMYPKDBS058']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRMYPKDBS058']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRMGLUDBS004
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRMGLUDBS004']),
        datetime.date(2012, 6, 26),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRMGLUDBS004']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRMGLUDBS004']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRLLISDBS030
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRLLISDBS030']),
        datetime.date(2014, 12, 4),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLLISDBS030']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRLLISDBS030']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRRDOEDBS057
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRRDOEDBS057']),
        datetime.date(2014, 10, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRRDOEDBS057']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRRDOEDBS057']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRCSMGDBS056
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCSMGDBS056']),
        datetime.date(2014, 7, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCSMGDBS056']), 3,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCSMGDBS056']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRCPFGDBS056
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCPFGDBS056']),
        datetime.date(2014, 7, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCPFGDBS056']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCPFGDBS056']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRCCRODBS095
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRCCRODBS095']),
        datetime.date(2014, 4, 15),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCCRODBS095']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRCCRODBS095']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRSBSPDBS148
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']),
        datetime.date(2014, 12, 20),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])
    debenture_criacao['amortizacao_carencia'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']),
                                                         datetime.date(2014, 12, 20),
                                                         debenture_criacao['amortizacao_carencia'])
    debenture_criacao['amortizacao_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']), 6,
                                                     debenture_criacao['amortizacao_cada'])
    debenture_criacao['amortizacao_taxa'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']), 12,
                                                     debenture_criacao['amortizacao_taxa'])
    debenture_criacao['amortizacao_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRSBSPDBS148']), 'MES',
                                                        debenture_criacao['amortizacao_unidade'])

    # Alteração específica isin = BRIPIPDBS017
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRIPIPDBS017']),
        datetime.date(2014, 6, 20),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRIPIPDBS017']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRIPIPDBS017']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRNATUDBS032
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRNATUDBS032']),
        datetime.date(2014, 8, 25),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRNATUDBS032']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRNATUDBS032']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRNATUDBS016
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRNATUDBS016']),
        datetime.date(2014, 8, 25),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRNATUDBS016']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRNATUDBS016']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRBRPRDBS043
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRBRPRDBS043']),
        datetime.date(2014, 6, 5),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRPRDBS043']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRPRDBS043']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRBRMLDBS050
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']),
        datetime.date(2013, 10, 26),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRLLISDBS022
    debenture_criacao['juros_criterio_novo_carencia'] = np.where(
        debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']),
        datetime.date(2013, 10, 4),
        debenture_criacao['juros_criterio_novo_carencia'])
    debenture_criacao['juros_criterio_novo_cada'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']), 6,
                                                             debenture_criacao['juros_criterio_novo_cada'])
    debenture_criacao['juros_criterio_novo_unidade'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRMLDBS050']),
                                                                'MES', debenture_criacao['juros_criterio_novo_unidade'])

    # Alteração específica isin = BRBRAPDBS098
    debenture_criacao['valor_nominal_na_emissao'] = np.where(debenture_criacao['codigo_isin'].isin(['BRBRAPDBS098']),
                                                             10000,
                                                             debenture_criacao['valor_nominal_na_emissao'])

    debenture_criacao['flag'] = 'C3'

    # Salva as alterações num excel
    alteracao = debenture[debenture.flag == 'C3'].copy()
    alteracao.to_excel(writer, 'criados_3')

    # Une com os outros papéis
    debenture = debenture[(debenture.tipo_de_amortizacao.notnull())].copy()
    debenture = debenture.append(debenture_criacao)

    # ----Criação de papéis que estão apenas ruins com as informações que vem da anbima_debentures
    # Alteração específica isin = BRENGTDBS009
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRENGTDBS009']),
                                                 datetime.date(2016, 4, 20), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['codigo_isin'].isin(['BRENGTDBS009']), 12,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_taxa'] = np.where(debenture['codigo_isin'].isin(['BRENGTDBS009']), 50,
                                             debenture['amortizacao_taxa'])
    debenture['amortizacao_unidade'] = np.where(debenture['codigo_isin'].isin(['BRENGTDBS009']), 'MES',
                                                debenture['amortizacao_unidade'])

    # Alteração específica isin = BRDASADBS011
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRDASADBS011']),
                                                 datetime.date(2014, 4, 29), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['codigo_isin'].isin(['BRDASADBS011']), 12,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_taxa'] = np.where(debenture['codigo_isin'].isin(['BRDASADBS011']), 1,
                                             debenture['amortizacao_taxa'])

    # Alteração específica isin = BRCTAXDBS005
    debenture['juros_criterio_novo_cada'] = np.where(debenture.codigo_isin.isin(['BRCTAXDBS005']), '6',
                                                     debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(debenture.codigo_isin.isin(['BRCTAXDBS005']), 'MES',
                                                        debenture['juros_criterio_novo_unidade'])
    debenture['juros_criterio_novo_carencia'] = np.where(debenture.codigo_isin.isin(['BRCTAXDBS005']),
                                                         datetime.date(2012, 6, 15),
                                                         debenture['juros_criterio_novo_carencia'])
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRCTAXDBS005']),
                                                 datetime.date(2015, 12, 15), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture.codigo_isin.isin(['BRCTAXDBS005']), '12',
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture.codigo_isin.isin(['BRCTAXDBS005']), 'MES',
                                                debenture['amortizacao_unidade'])
    debenture['data_de_vencimento'] = np.where(debenture['codigo_isin'].isin(['BRCTAXDBS005']),
                                               datetime.date(2016, 12, 15),
                                               debenture['data_de_vencimento'])

    # Alteração específica isin = BRMYPKDBS025
    debenture['juros_criterio_novo_cada'] = np.where(debenture.codigo_isin.isin(['BRMYPKDBS025']), '6',
                                                     debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(debenture.codigo_isin.isin(['BRMYPKDBS025']), 'MES',
                                                        debenture['juros_criterio_novo_unidade'])
    debenture['juros_criterio_novo_carencia'] = np.where(debenture.codigo_isin.isin(['BRMYPKDBS025']),
                                                         datetime.date(2012, 6, 15),
                                                         debenture['juros_criterio_novo_carencia'])
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRMYPKDBS025']),
                                                 datetime.date(2015, 12, 15), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture.codigo_isin.isin(['BRMYPKDBS025']), '12',
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture.codigo_isin.isin(['BRMYPKDBS025']), 'MES',
                                                debenture['amortizacao_unidade'])
    debenture['data_de_vencimento'] = np.where(debenture['codigo_isin'].isin(['BRMYPKDBS025']),
                                               datetime.date(2016, 12, 15),
                                               debenture['data_de_vencimento'])

    # Alteração específica isin = BRGEPADBS053
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRGEPADBS053']),
                                                 datetime.date(2016, 1, 10), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['codigo_isin'].isin(['BRGEPADBS053']), 12,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_taxa'] = np.where(debenture['codigo_isin'].isin(['BRGEPADBS053']), 1,
                                             debenture['amortizacao_taxa'])

    # Alteração específica isin = BRCBEEDBS062
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRCBEEDBS062']),
                                                 datetime.date(2015, 6, 15), debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['codigo_isin'].isin(['BRCBEEDBS062']), 12,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_taxa'] = np.where(debenture['codigo_isin'].isin(['BRCBEEDBS062']), 1,
                                             debenture['amortizacao_taxa'])

    # Alteração específica isin = BRBISADBS067
    debenture['amortizacao_carencia'] = np.where(debenture['codigo_isin'].isin(['BRBISADBS067']),
                                                 datetime.date(2015, 8, 8),
                                                 debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['codigo_isin'].isin(['BRBISADBS067']), 12,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_taxa'] = np.where(debenture['codigo_isin'].isin(['BRBISADBS067']), 1,
                                             debenture['amortizacao_taxa'])

    # codigo_isin BRESCEDBS025
    debenture['juros_criterio_novo_cada'] = np.where(debenture.codigo_isin.isin(['BRESCEDBS025']), '6',
                                                     debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(debenture.codigo_isin.isin(['BRESCEDBS025']), 'MES',
                                                        debenture['juros_criterio_novo_unidade'])
    debenture['amortizacao_cada'] = np.where(debenture.codigo_isin.isin(['BRESCEDBS025']), '6',
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture.codigo_isin.isin(['BRESCEDBS025']), 'MES',
                                                debenture['amortizacao_unidade'])

    # codigo_isin BRINBDDBS013
    debenture['juros_criterio_novo_cada'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), '12',
                                                     debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), 'MES',
                                                        debenture['juros_criterio_novo_unidade'])
    debenture['amortizacao_cada'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), '12',
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), 'MES',
                                                debenture['amortizacao_unidade'])
    debenture['juros_criterio_novo_prazo'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), '252',
                                                      debenture['juros_criterio_novo_prazo'])
    debenture['juros_criterio_novo_taxa'] = np.where(debenture.codigo_isin.isin(['BRINBDDBS013']), '4.5',
                                                     debenture['juros_criterio_novo_taxa'])

    # codigo_isin BRUNIDDBS021
    debenture['juros_criterio_novo_cada'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), '12',
                                                     debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), 'MES',
                                                        debenture['juros_criterio_novo_unidade'])
    debenture['amortizacao_cada'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), '12',
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), 'MES',
                                                debenture['amortizacao_unidade'])
    debenture['juros_criterio_novo_prazo'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), '252',
                                                      debenture['juros_criterio_novo_prazo'])
    debenture['juros_criterio_novo_taxa'] = np.where(debenture.codigo_isin.isin(['BRUNIDDBS021']), '4.5',
                                                     debenture['juros_criterio_novo_taxa'])

    # ----Finalização e carregamento na bd

    # Conversão codigo_isin -> isin
    debenture = debenture.rename(columns={'codigo_isin': 'isin'})

    # Preenche vazios
    debenture['amortizacao_carencia'] = np.where(debenture['amortizacao_carencia'].isnull(),
                                                 debenture['data_de_vencimento'], debenture['amortizacao_carencia'])
    debenture['amortizacao_cada'] = np.where(debenture['amortizacao_carencia'] == debenture['data_de_vencimento'], None,
                                             debenture['amortizacao_cada'])
    debenture['amortizacao_unidade'] = np.where(debenture['amortizacao_carencia'] != debenture['data_de_vencimento'],
                                                'MES',
                                                None)

    debenture['juros_criterio_novo_taxa'] = np.where(debenture['juros_criterio_novo_taxa'].isnull(), 0,
                                                     debenture['juros_criterio_novo_taxa'])
    debenture['juros_criterio_novo_cada'] = np.where(
        debenture['juros_criterio_novo_carencia'] == debenture['data_de_vencimento'], None,
        debenture['juros_criterio_novo_cada'])
    debenture['juros_criterio_novo_unidade'] = np.where(
        debenture['juros_criterio_novo_carencia'] == debenture['data_de_vencimento'], None,
        debenture['juros_criterio_novo_unidade'])

    debenture['juros_criterio_novo_tipo'] = np.where(debenture['juros_criterio_novo_tipo'] != 'Linear', 'Exponencial',
                                                     debenture['juros_criterio_novo_tipo'])

    debenture['id_papel'] = debenture['id_papel'] + '_' + debenture['flag']

    debenture[['id_papel',
               'isin',
               'flag',
               'data_de_emissao',
               'data_de_vencimento',
               'juros_criterio_novo_carencia',
               'juros_criterio_novo_cada',
               'juros_criterio_novo_unidade',
               'juros_criterio_novo_taxa',
               'percentual_multiplicador_rentabilidade',
               'amortizacao_carencia',
               'amortizacao_cada',
               'amortizacao_unidade',
               'tipo_de_amortizacao',
               'amortizacao_taxa']].to_excel(writer, 'todos_col_reduzida')

    # Salva as todos num excel
    debenture.to_excel(writer, 'todos')

    # Deleta columnas não existentes na bd anbima_debentures
    del debenture['puemissao']
    del debenture['percindex']
    del debenture['dtvencimento']
    del debenture['dtemissao']
    del debenture['coupom']
    del debenture['indice_xml']
    del debenture['codigo_do_ativo_xml']

    # Salva excel com todas as abas
    writer.save()
    logger.info("Arquivos salvos com sucesso")

    # Cria a coluna com a data da carga
    debenture['data_bd'] = horario_bd

    logger.info("Salvando base de dados - debentures_caracteristicas")
    pd.io.sql.to_sql(debenture, name='debentures_caracteristicas', con=connection, if_exists="append", flavor='mysql', index=False)

    connection.close()