def bmf_numeraca_ajustes_cadastro():

    import pandas as pd
    import datetime
    import pymysql as db
    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Diretório de dependencias
    depend_path_carat = full_path_from_database('excels') + 'caracteristicas.xlsx'
    depend_path_bullet = full_path_from_database('excels') + 'bullet_12meses.xlsx'

    # Diretório de save de planilhas
    xlsx_path = full_path_from_database('get_output_quadro419') + 'controle_titprivado.xlsx'

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    manual = pd.read_excel(depend_path_carat)

    horario_bd = datetime.datetime.now()

    writer = ExcelWriter(xlsx_path)

    '''------------------------------------------------------------------------

                        Leitura e tratamento das tabelas

    ------------------------------------------------------------------------'''

    ###########################################################################
    # ----Leitura da base numeraca
    ###########################################################################

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    query = 'SELECT t.* FROM (SELECT codigo_isin, MAX(data_bd) AS max_data FROM bmf_numeraca GROUP BY codigo_isin ) AS m INNER JOIN bmf_numeraca AS t ON t.codigo_isin = m.codigo_isin AND t.data_bd = m.max_data'

    titprivado_bmf = pd.read_sql(query, con=connection)

    ###########################################################################
    # ----Tratamento da tabela bmf
    ###########################################################################

    # Criação do flag 'O' - existe na numeraca
    titprivado_bmf['flag'] = 'O'

    # Remove duplicatas
    titprivado_bmf = titprivado_bmf.sort(['codigo_isin', 'data_bd'], ascending=[True, True])
    titprivado_bmf = titprivado_bmf.drop_duplicates(subset=['codigo_isin'], take_last=True)

    # Deleta colunas não necessárias
    del titprivado_bmf['data_bd']

    ###########################################################################
    # ----Leitura da tabela xml
    ###########################################################################

    query = 'SELECT * FROM projeto_inv.xml_titprivado_org '

    titprivado_xml = pd.read_sql(query, con=connection)

    # Seleciona última carga com dtposicao = data_base
    titprivado_xml['dtrel'] = titprivado_xml['id_papel'].str.split('_')
    titprivado_xml['dtrel'] = titprivado_xml['dtrel'].str[0]

    titprivado_xml = titprivado_xml[titprivado_xml.dtrel == dtbase_concat].copy()
    titprivado_xml = titprivado_xml[titprivado_xml.data_bd == max(titprivado_xml.data_bd)]

    query = 'SELECT * FROM projeto_inv.xml_header_org '

    header_xml = pd.read_sql(query, con=connection)

    # Seleciona última carga com dtposicao = data_base
    header_xml = header_xml[
        header_xml.dtposicao == datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2]))].copy()
    header_xml = header_xml[header_xml.data_bd == max(header_xml.data_bd)]

    ###########################################################################
    # ----Tratamento da tabela xml
    ###########################################################################

    # Renomeação de colunas
    titprivado_xml = titprivado_xml.rename(columns={'isin': 'codigo_isin', 'indexador': 'indexador_xml'})

    # Seleção das colunas necessárias
    titprivado_xml = titprivado_xml[['codigo_isin',
                                     'codativo',
                                     'dtemissao',
                                     'dtoperacao',
                                     'dtvencimento',
                                     'puemissao',
                                     'coupom',
                                     'indexador_xml',
                                     'percindex',
                                     'id_papel']]

    ###########################################################################
    # ----Criação da tabela xml + numeraca
    ###########################################################################

    # Constrói base xml+numeraca
    titprivado = pd.merge(titprivado_xml, titprivado_bmf, left_on=['codigo_isin'], right_on=['codigo_isin'], how='left')

    # Cria a coluna com a data da carga
    titprivado['data_bd'] = horario_bd

    # Arruma os NaN
    titprivado = titprivado.where((pd.notnull(titprivado)), None)

    # Seleciona e salva na bd as características não alteradas
    titprivado_original_bmf = titprivado[titprivado.flag.notnull()].copy()

    # Reseta o indice do data_frame
    titprivado = titprivado.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    titprivado_original_bmf = titprivado_original_bmf.reset_index(level=None, drop=True, inplace=False, col_level=0,
                                                                  col_fill='')
    # Deleta columnas não existentes na bd bmf_numeraca
    del titprivado_original_bmf['puemissao']
    del titprivado_original_bmf['percindex']
    del titprivado_original_bmf['dtvencimento']
    del titprivado_original_bmf['dtemissao']
    del titprivado_original_bmf['coupom']
    del titprivado_original_bmf['indexador_xml']
    del titprivado_original_bmf['codativo']

    # Contatena tabelas
    titprivado_original_bmf['id_papel'] = titprivado_original_bmf['id_papel'] + '_' + titprivado_original_bmf['flag']

    pd.io.sql.to_sql(titprivado_original_bmf, name='titprivado_caracteristicas', con=connection, if_exists="append",
                     flavor='mysql', index=False)

    connection.close()

    del titprivado_original_bmf

    '''------------------------------------------------------------------------

                    Manipulação das informações cadastrais

    ------------------------------------------------------------------------'''

    ###############################################################################
    # Verificação dos papéis não existentes na numeraca
    ###############################################################################
    erros = titprivado[titprivado.flag.isnull()].copy()
    erros.to_excel(writer, 'fora_numeraca')

    ###############################################################################
    # Criação de informações  - fonte: xml - ASSUME BULLET, joga fora papéis já vencidos
    ###############################################################################\
    idx = titprivado[(titprivado.flag.isnull()) & (titprivado.dtvencimento >= datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2])))].index.tolist()

    titprivado['data_emissao'][idx] = titprivado['dtemissao'][idx]
    titprivado['data_expiracao'][idx] = titprivado['dtvencimento'][idx]
    titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_expiracao'][idx]
    titprivado['valor_nominal'][idx] = titprivado['puemissao'][idx]
    titprivado['indexador'][idx] = titprivado['indexador_xml'][idx]
    titprivado['percentual_indexador'][idx] = titprivado['percindex'][idx]
    titprivado['taxa_juros'][idx] = titprivado['coupom'][idx]
    titprivado['ano_expiracao'][idx] = titprivado['data_expiracao'][idx].astype(str)
    titprivado['ano_expiracao'][idx] = titprivado['ano_expiracao'][idx].str.split('-')
    titprivado['ano_expiracao'][idx] = titprivado['ano_expiracao'][idx].str[0].astype(int)
    titprivado['flag'][idx] = 'C0'

    erros = titprivado[titprivado.flag == 'C0'].copy()
    erros.to_excel(writer, 'criados_0')

    ###############################################################################
    # Criação de informações  - fonte: fluxo manual
    ###############################################################################

    manual['data_primeiro_pagamento_juros1'] = pd.to_datetime(manual['data_primeiro_pagamento_juros1']).dt.date

    titprivado = titprivado.merge(manual, on=['codigo_isin'], how='left')

    idx = titprivado[titprivado.taxa_juros1.notnull()].index.tolist()

    titprivado['cod_frequencia_juros'][idx] = titprivado['cod_frequencia_juros1'][idx]
    titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_primeiro_pagamento_juros1'][idx]
    titprivado['valor_nominal'][idx] = titprivado['valor_nominal1'][idx]
    titprivado['indexador'][idx] = titprivado['indexador1'][idx]
    titprivado['percentual_indexador'][idx] = titprivado['percentual_indexador1'][idx]
    titprivado['taxa_juros'][idx] = titprivado['taxa_juros1'][idx]
    titprivado['tipo_ativo'][idx] = titprivado['tipo_ativo1'][idx]
    titprivado['flag'][idx] = 'C1'

    del titprivado['cod_frequencia_juros1']
    del titprivado['data_primeiro_pagamento_juros1']
    del titprivado['valor_nominal1']
    del titprivado['indexador1']
    del titprivado['percentual_indexador1']
    del titprivado['taxa_juros1']
    del titprivado['tipo_ativo1']

    erros = titprivado[titprivado.flag == 'C1'].copy()
    erros.to_excel(writer, 'criados_1')

    ###############################################################################
    # Manipulação V - Preenche valor nominal com info do xml
    ###############################################################################

    # Papéis que tem informação diferente do xml
    titprivado['flag'][(titprivado.flag != 'C0') & (titprivado.flag != 'C1')] = 'V'
    titprivado['valor_nominal'][titprivado.flag == 'V'] = titprivado['puemissao'][titprivado.flag == 'V']

    erros = titprivado[titprivado.flag == 'V'].copy()
    erros.to_excel(writer, 'valor_nominal')

    ###############################################################################
    # Manipulação I - Preenche percentual indexador com info do xml
    ###############################################################################

    # Caso o percentual_indexador seja missing, preenche com a inforção do xml
    titprivado['flag'][
        (titprivado.flag != 'C0') & (titprivado.flag != 'C1') & (titprivado.percentual_indexador.isnull())] = 'I'
    titprivado['percentual_indexador'][titprivado.flag == 'I'] = titprivado['percindex'][titprivado.flag == 'I']

    erros = titprivado[titprivado.flag == 'I'].copy()
    erros.to_excel(writer, 'percentual_indexador')

    ###############################################################################
    # Manipulação J0 - Preenche taxa de juros com info do xml, quando percentual indexador = 100
    ###############################################################################

    # Caso o taxa_juros seja missing, preenche com a inforção do xml
    titprivado['flag'][(titprivado.flag != 'C0') & (titprivado.flag != 'C1') & (
    titprivado.taxa_juros.isnull() & (titprivado.percentual_indexador == 100))] = 'J0'
    titprivado['taxa_juros'][(titprivado.flag == 'J0')] = titprivado['coupom'][(titprivado.flag == 'J0')]

    erros = titprivado[titprivado.flag == 'J0'].copy()
    erros.to_excel(writer, 'taxa_juros_0')

    ###############################################################################
    # Manipulação J0 - Preenche taxa de juros com info do xml, quando percentual indexador != 100
    ###############################################################################

    # Caso o taxa_juros seja missing, preenche com a inforção do xml
    titprivado['flag'][(titprivado.flag != 'C0') & (titprivado.flag != 'C1') & (
    titprivado.taxa_juros.isnull() & (titprivado.percentual_indexador != 100))] = 'J1'
    titprivado['taxa_juros'][titprivado.flag == 'J1'] = 0

    erros = titprivado[titprivado.flag == 'J1'].copy()
    erros.to_excel(writer, 'taxa_juros_1')

    ###############################################################################
    # Missing data_primeiro_pagamento_juros
    #
    # No programa de fluxo, fiz uma modificação para os papéis que tinham código de frequência.
    # Coloquei a data de primeiro pagamento como sendo a próxima após a data de emissão depois de um período correspondente ao código de frequência de juros
    ###############################################################################

    # Caso o papel tenha um código de frequência de juros, preenche a primeira data de pagamento com a primeira data depois do periodo considerando o cod_frequencia_juros
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='B')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='B')] + pd.DateOffset(years=2)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'B'))] = 'D'
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='A')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='A')] + pd.DateOffset(years=1)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'A'))] = 'D'
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='S')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='S')] + pd.DateOffset(months=6)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'S'))] = 'D'
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='Q')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='Q')] + pd.DateOffset(months=3)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'Q'))] = 'D'
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='M')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='M')] + pd.DateOffset(months=1)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'M'))] = 'D'
    # titprivado['data_primeiro_pagamento_juros'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='W')] = titprivado['data_emissao'][(titprivado.data_primeiro_pagamento_juros.isnull())&(titprivado.cod_frequencia_juros=='W')] + pd.DateOffset(days=7)
    titprivado['flag'][(titprivado.flag != 'C') & (
    (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'W'))] = 'D'

    erros = titprivado[titprivado.flag == 'D'].copy()
    erros.to_excel(writer, 'data_primeiro_pagamento_juros')

    ###############################################################################
    # Faz substituição do código de frequência de juros para os isins especificados no excel
    ###############################################################################

    # Lista de papéis na transformação: JUROS '' bullet -> 'A' anual
    isin_list = pd.read_excel(depend_path_bullet)
    titprivado = titprivado.merge(isin_list, on=['codigo_isin'], how='left')
    titprivado['cod_frequencia_juros'][titprivado.flag_err == 1] = 'A'
    titprivado['flag'][titprivado.flag_err == 1] = 'B0'
    del titprivado['flag_err']

    erros = titprivado[titprivado.flag == 'B0'].copy()
    erros.to_excel(writer, 'cod_frequencia_juros_bullet')

    # Lista de papéis na transformação: JUROS '' bullet -> 'A' anual
    isin_list = pd.read_excel(depend_path_bullet)
    titprivado = titprivado.merge(isin_list, on=['codigo_isin'], how='left')
    titprivado['cod_frequencia_juros'][titprivado.flag_err == 1] = None
    titprivado['flag'][titprivado.flag_err == 1] = 'B1'
    del titprivado['flag_err']

    erros = titprivado[titprivado.flag == 'B1'].copy()
    erros.to_excel(writer, 'cod_frequencia_juros_12meses')

    ###############################################################################
    # Correção ultra-manual
    ###############################################################################

    titprivado['valor_nominal'][titprivado.codigo_isin == 'BRBCXGDP00J9'] = 2062500.0
    titprivado['flag'][titprivado.codigo_isin == 'BRBCXGDP00J9'] = 'M'
    titprivado['valor_nominal'][titprivado.codigo_isin == 'BRCSCSDP0075'] = 1000000.0
    titprivado['flag'][titprivado.codigo_isin == 'BRCSCSDP0075'] = 'M'
    titprivado['percentual_indexador'][titprivado.codigo_isin == 'BRBCEFC01JQ9'] = 102.5
    titprivado['flag'][titprivado.codigo_isin == 'BRBCEFC01JQ9'] = 'M'
    titprivado['percentual_indexador'][titprivado.codigo_isin == 'BRBITALFID01'] = 100.0
    titprivado['flag'][titprivado.codigo_isin == 'BRBITALFID01'] = 'M'
    titprivado['ano_expiracao'][titprivado.codigo_isin == 'BRBPNMLFI0L5'] = 2018
    titprivado['flag'][titprivado.codigo_isin == 'BRBPNMLFI0L5'] = 'M'
    titprivado['ano_expiracao'][titprivado.codigo_isin == 'BRFTORDP0045'] = 2017
    titprivado['flag'][titprivado.codigo_isin == 'BRFTORDP0045'] = 'M'
    titprivado['ano_expiracao'][titprivado.codigo_isin == '************'] = 2017
    titprivado['flag'][titprivado.codigo_isin == '************'] = 'M'
    titprivado['tipo_ativo'][titprivado.codigo_isin == 'BRMULTCC0016'] = 'C'
    titprivado['flag'][titprivado.codigo_isin == 'BRMULTCC0016'] = 'M'

    erros = titprivado[titprivado.flag == 'M'].copy()
    erros.to_excel(writer, 'manual')

    ###############################################################################
    # Finalização
    ###############################################################################

    # Retira papéis que estão fora da numeraca e que não foram criados
    titprivado = titprivado[titprivado.flag.notnull()].copy()

    # Coloca o flag bo id_papel
    titprivado['id_papel'] = titprivado['id_papel'] + '_' + titprivado['flag']

    # Todos os papéis
    titprivado.to_excel(writer, 'todos')

    # Salva excel com todas as abas
    writer.save()

    # Deleta columnas não existentes na bd bmf_numeraca
    del titprivado['puemissao']
    del titprivado['percindex']
    del titprivado['dtvencimento']
    del titprivado['dtemissao']
    del titprivado['coupom']
    del titprivado['indexador_xml']
    del titprivado['codativo']

    ###############################################################################
    # Salvar informacoes alteradas no banco de dados
    ###############################################################################

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    pd.io.sql.to_sql(titprivado, name='titprivado_caracteristicas', con=connection, if_exists="append", flavor='mysql',
                     index=False)

    connection.close()
