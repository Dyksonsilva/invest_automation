def mtm_titulo_debenture():

    import datetime
    import pandas as pd
    import numpy as np
    import pymysql as db
    from findt import FinDt
    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    ###############################################################################
    # 1 - Declaração de caminhos
    ###############################################################################

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    # Diretório de dependencias
    depend_path_manual_fidc = full_path_from_database('excels') + 'fluxo_manual_fidc.xlsx'
    depend_path_feriados_nacionais = full_path_from_database('feriados_nacionais') + 'feriados_nacionais.csv'

    # Diretório de save de planilhas
    save_path_erros = full_path_from_database('get_output_quadro419') + 'erros_mtm_titprivado.xlsx'
    save_path_mtm_titprivado = full_path_from_database('get_output_quadro419') + 'mtm_titprivado_debentures.xlsx'

    ###############################################################################
    # 2 - Cria conexão e importação: base de dados fluxo_titprivado
    ###############################################################################

    # Data base de cálculo
    # dt_base = "20160831"
    ano = str(dtbase_concat)[0:4]
    mes = str(dtbase_concat)[4:6]
    dia = str(dtbase_concat)[6:8]
    dtbase_mtm = datetime.date(int(ano), int(mes), int(dia))

    # Query para puxar apenas os papéis que ainda não venceram
    query = 'SELECT * FROM projeto_inv.fluxo_titprivado WHERE data_expiracao>=' + '"' + dtbase_concat + '";'

    # Importação dos dados
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')
    base_fluxo = pd.read_sql(query, con=connection)
    connection.close()

    base_fluxo['dtrel'] = base_fluxo['id_papel'].str.split('_')
    base_fluxo['dtrel'] = base_fluxo['dtrel'].str[0]

    base_fluxo_deb = base_fluxo[base_fluxo.tipo_ativo == 'DBS'].copy()
    if len(base_fluxo_deb) != 0:
        base_fluxo_deb = base_fluxo_deb[base_fluxo_deb.dtrel == dtbase_concat].copy()
        base_fluxo_deb = base_fluxo_deb[base_fluxo_deb.data_bd == max(base_fluxo_deb.data_bd)].copy()

    base_fluxo_tit = base_fluxo[base_fluxo.tipo_ativo != 'DBS'].copy()
    if len(base_fluxo_tit) != 0:
        base_fluxo_tit = base_fluxo_tit[base_fluxo_tit.dtrel == dtbase_concat].copy()
        base_fluxo_tit = base_fluxo_tit[base_fluxo_tit.data_bd == max(base_fluxo_tit.data_bd)].copy()

    base_fluxo = base_fluxo_tit.append(base_fluxo_deb)

    # Puxa os fluxos de FIDC - BASE ESTÁTICA
    fluxo_fidc = pd.read_excel(depend_path_manual_fidc)

    fluxo_fidc['id_papel'] = base_fluxo['dtrel'].iloc[0] + '_' + fluxo_fidc['cod_frequencia_juros'].astype(str)

    # Muda formato
    fluxo_fidc['data_emissao'] = pd.to_datetime(fluxo_fidc['data_emissao'])
    fluxo_fidc['data_expiracao'] = pd.to_datetime(fluxo_fidc['data_expiracao'])
    fluxo_fidc['data_primeiro_pagamento_juros'] = pd.to_datetime(fluxo_fidc['data_primeiro_pagamento_juros'])
    fluxo_fidc['dt_inicio_rentab'] = pd.to_datetime(fluxo_fidc['dt_inicio_rentab'])
    fluxo_fidc['dt_ref'] = pd.to_datetime(fluxo_fidc['dt_ref'])

    fluxo_fidc['valor_nominal'] = fluxo_fidc['valor_nominal'].astype(float)

    # Força a data de operação à data de emissao do fidc - não impacta a conta pq não há o campo característica para cotas
    fluxo_fidc['dtoperacao'] = fluxo_fidc['data_emissao']

    # Adiciona o fluxo de fidc à base_fluxo
    base_fluxo = base_fluxo.append(fluxo_fidc)

    # base_fluxo['indexador'] = base_fluxo['indexador'].str.replace('DI','DI1')
    base_fluxo['indexador'] = base_fluxo['indexador'].str.replace('IAP', 'IPCA')
    base_fluxo['indexador'] = base_fluxo['indexador'].str.replace('SEM-ÍNDICE', 'PRE')
    base_fluxo['indexador'] = base_fluxo['indexador'].str.replace('ANB', 'DI1')
    base_fluxo['indexador'] = base_fluxo['indexador'].str.replace('ANBID', 'DI1')

    ###############################################################################
    # PAPÉIS TJLP
    base_fluxo['indexador'] = np.where(base_fluxo['codigo_isin'].isin(['BRBNDPDBS0B9']), 'TJLP', base_fluxo['indexador'])
    ###############################################################################

    ###############################################################################
    # Redução da base
    # base_fluxo = base_fluxo[base_fluxo.codigo_isin.isin(['BRMGIPDBS000'])].copy()
    ###############################################################################

    ###############################################################################
    # 3 - Criação das linhas: data-base de cálculo, dtoperacao e, para debentures, dtspread
    ###############################################################################

    # Puxa as informações de negociação em mercado secuindário da Anbima para debentures -> linha dtspread
    query = 'select * from projeto_inv.anbima_debentures'

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    anbima_deb = pd.read_sql(query, con=connection)

    connection.close()

    anbima_deb = anbima_deb[anbima_deb.data_referencia <= dtbase_mtm]

    anbima_deb = anbima_deb.sort(['codigo', 'data_referencia', 'data_bd'], ascending=[True, True, True])
    anbima_deb = anbima_deb.drop_duplicates(subset=['codigo'], take_last=True)

    anbima_deb = anbima_deb[['codigo', 'data_referencia']].copy()

    anbima_deb['codigo'] = anbima_deb['codigo'].astype(str)

    # Puxa informações da debentures_caracteristicas para cruzar isin com codigo_ativo
    query = 'select * from projeto_inv.debentures_caracteristicas'

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    deb_carac = pd.read_sql(query, con=connection)

    connection.close()

    deb_carac = deb_carac[['isin', 'codigo_do_ativo', 'data_bd', 'registro_cvm_da_emissao']].copy()

    deb_carac = deb_carac.sort(['isin', 'codigo_do_ativo', 'data_bd'], ascending=[True, True, True])
    deb_carac = deb_carac.drop_duplicates(subset=['isin', 'codigo_do_ativo'], take_last=True)

    deb_carac = deb_carac.rename(columns={'isin': 'codigo_isin', 'codigo_do_ativo': 'codigo'})

    deb_carac['codigo'] = deb_carac['codigo'].astype(str)
    deb_carac['codigo'] = deb_carac['codigo'].str.replace(' ', '')

    del deb_carac['data_bd']

    # Une as informações das caracteristicas com as do mercado secundário
    anbima_deb = anbima_deb.merge(deb_carac, on=['codigo'], how='left')
    anbima_deb = anbima_deb[anbima_deb.codigo_isin.notnull()]

    anbima_deb = anbima_deb.sort(['data_referencia'], ascending=True)
    anbima_deb = anbima_deb.drop_duplicates(subset=['codigo_isin'], take_last=True)

    del anbima_deb['codigo']

    # Une anbima_deb à base de fluxo
    base_fluxo = base_fluxo.merge(anbima_deb, on=['codigo_isin'], how='left')

    # Data frame com apenas uma linha de cada papel com as informaçoes fixas
    base_fluxo = base_fluxo.sort(['codigo_isin', 'id_papel', 'flag', 'dt_ref', 'data_bd'],
                                 ascending=[True, True, True, True, False])

    base_fluxo['dtoperacao'][base_fluxo.dtoperacao < base_fluxo.data_emissao] = base_fluxo['data_emissao'][
        base_fluxo.dtoperacao < base_fluxo.data_emissao]

    # Atualiza o id_papel para diferenciar de DBS e de TITPRIVADO
    base_fluxo['id_papel'] = base_fluxo['id_papel'] + '-' + base_fluxo['codigo_isin']

    base_data = base_fluxo.drop_duplicates(subset=['codigo_isin',
                                                   'codigo_cetip',
                                                   'id_bmf_numeraca',
                                                   'id_anbima_debentures',
                                                   'tipo_ativo',
                                                   'data_emissao',
                                                   'data_expiracao',
                                                   'indexador',
                                                   'percentual_indexador',
                                                   'cod_frequencia_juros',
                                                   'data_primeiro_pagamento_juros',
                                                   'id_papel',
                                                   'flag',
                                                   'dtoperacao',
                                                   'data_referencia'
                                                   ], take_last=False)

    base_data1 = base_data.copy()
    base_data2 = base_data.copy()

    # Preenche as colunas relacionadas com tipo/característica de evento como se não houvesse nenhum evento
    ###############################################################################
    # accrue1 = para não deletar as linhas com dt_ref duplicadas por causa do fluxo desconpassado da amortização com relação aos pagamentos de juros
    base_data['dt_ref'] = dtbase_mtm
    base_data['juros_tipo'] = 'accrue'
    base_data['index_tipo'] = 'accrue'
    base_data['flag_inclusao'] = 2
    base_data['perc_amortizacao'] = 0

    base_data1['dt_ref'] = base_data1['dtoperacao']
    base_data1['juros_tipo'] = 'accrue'
    base_data1['index_tipo'] = 'accrue'
    base_data1['flag_inclusao'] = 1
    base_data1['perc_amortizacao'] = 0

    base_data2['dt_ref'] = base_data2['data_referencia']
    base_data2['juros_tipo'] = 'accrue'
    base_data2['index_tipo'] = 'accrue'
    base_data2['flag_inclusao'] = 3
    base_data2['perc_amortizacao'] = 0
    base_data2 = base_data2[base_data2.data_referencia.notnull()].copy()

    base_data = base_data.append(base_data1)
    base_data = base_data.append(base_data2)
    ###############################################################################

    # Cria no data frame do fluxo a coluna de flag de inclusão de linha de data base de cálculo
    #   flag_inclusão = 1 -> linha inserida
    #   flag_inclusão =0 -> linha já existente
    base_fluxo['flag_inclusao'] = 0
    base = base_fluxo.append(base_data)
    base['flag_inclusao'] = base['flag_inclusao'].fillna(0)

    base['dt_ref1'] = pd.to_datetime(base['dt_ref'])

    # Se tiver pagamento previsto exatamente na data de calculo de mtm, privilegiar informação original
    base = base.sort(['codigo_isin', 'id_papel', 'flag', 'dt_ref1', 'data_bd', 'flag_inclusao'],
                     ascending=[True, True, True, True, False, True])
    base_mtm = base.drop_duplicates(subset=['codigo_isin', 'id_papel', 'flag', 'dt_ref1'], take_last=False)
    base_mtm = base_mtm.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # Inversão das colunas para chave única
    base_mtm['codigo_isin_temp'] = base_mtm['codigo_isin']
    base_mtm['codigo_isin'] = base_mtm['id_papel']
    base_mtm['id_papel'] = base_mtm['codigo_isin_temp']

    del base_mtm['codigo_isin_temp']

    ###############################################################################
    # 4 - Tratamento das datas de início e fim dos períodos de capitalização
    ###############################################################################

    # AJUSTES
    # --Acertar o gerador de fluxo numeraca - percentual de amortização - fluxo ainda tem problemas
    # Chamber: deleta linha extra além da data de expiração
    # base_mtm = base_mtm[base_mtm.dt_ref1<=base_mtm.data_expiracao]
    # TypeError: Cannot change data-type for object array.

    ###############################################################################
    # Seleciona amostra da bd
    # data_aux_aux = base_mtm.copy()
    # del base_mtm
    # rows = np.random.choice(data_aux_aux.index.values, 10)
    #
    # data_aux_aux = data_aux_aux.ix[rows]
    #
    # data_aux = data_aux_aux.ix[rows]
    ##data_aux = data_aux.drop_duplicates(subset=['codigo_isin'],take_last=False)
    #
    # base_mtm = data_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    #
    # del data_aux_aux
    ###############################################################################

    # Preenche com tipo_capitalizacao com Exponencial quando não está especificado
    base_mtm['tipo_capitalizacao'] = np.where(base_mtm['tipo_capitalizacao'].isnull(), 'Exponencial',
                                              base_mtm['tipo_capitalizacao'])
    base_mtm['tipo_capitalizacao'] = np.where(base_mtm['tipo_capitalizacao'] == 'None', 'Exponencial',
                                              base_mtm['tipo_capitalizacao'])

    # Preenche com percentual_indexador com 100 quando não está especificado ou está zerado
    base_mtm['percentual_indexador'] = np.where(base_mtm['percentual_indexador'].isnull(), 100,
                                                base_mtm['percentual_indexador'])
    base_mtm['percentual_indexador'] = np.where(base_mtm['percentual_indexador'] == 0, 100,
                                                base_mtm['percentual_indexador'])

    # Divide percentual juros/100 se for >1
    base_mtm['percentual_juros'] = np.where(base_mtm['percentual_juros'] > 1, base_mtm['percentual_juros'] / 100,
                                            base_mtm['percentual_juros'])

    # Preenche a data de inicio de rentabilidade do fluxo numeraca
    base_mtm['dt_inicio_rentab'][base_mtm.dt_inicio_rentab.isnull()] = base_mtm['data_emissao'][
        base_mtm.dt_inicio_rentab.isnull()]
    # Seleciona estes indexadores para fazer o tratamento
    mtm = base_mtm[base_mtm['indexador'].isin(['IPCA', 'IGP', 'DI1', 'PRE', 'TJLP'])]

    mtm = mtm.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    mtm['dt_ref1'] = pd.to_datetime(mtm['dt_ref1'])
    mtm['dt_fim'] = mtm['dt_ref1']

    # Para ficar com as datas de início e de fim do período de capitalização
    mtm['dt_fim_lag'] = mtm['dt_fim'].shift(1)
    mtm['codigo_isin_lag'] = mtm['codigo_isin'].shift(1)
    # Tem o pd.DateOffset de um dia para não capitalizar duas vezes o mesmo dia
    mtm['dt_inicio'] = np.where(mtm['codigo_isin'] == mtm['codigo_isin_lag'], pd.to_datetime(mtm['dt_fim_lag']),
                                pd.to_datetime(mtm['dt_inicio_rentab']))
    mtm['dt_inicio'].iloc[0] = mtm['dt_inicio_rentab'].iloc[0]

    # mtm['dt_fim'] = mtm['dt_fim'].dt.date
    # aux1 = mtm[(mtm.dt_fim==mtm.dtoperacao)&(mtm.flag!='O')]
    # aux2 = mtm[(mtm.dt_ref==dt_base_mtm)&(mtm.flag!='O')]



    # Quando é DI, a data de cálculo é exclusive
    # Toma cuidado com a data de cálculo
    mtm['dt_inicio'] = np.where(mtm['indexador'] == 'DI1', mtm['dt_inicio'] - pd.DateOffset(months=0, days=1),
                                mtm['dt_inicio'])
    mtm['dt_fim'] = np.where(mtm['indexador'] == 'DI1', mtm['dt_fim'] - pd.DateOffset(months=0, days=1), mtm['dt_fim'])

    # Cria colunas com ano, mês e dia da data de inicio
    mtm['ano_inicio'] = mtm['dt_inicio'].dt.year
    mtm['mes_inicio'] = mtm['dt_inicio'].dt.month
    mtm['dia_inicio'] = mtm['dt_inicio'].dt.day

    # Cria colunas com ano, mês e dia da data de fim
    mtm['ano_fim'] = mtm['dt_fim'].dt.year
    mtm['mes_fim'] = mtm['dt_fim'].dt.month
    mtm['dia_fim'] = mtm['dt_fim'].dt.day

    ###############################################################################
    # 5 - Tratamento da base de indexadores de atualização de Valor Nominal
    ###############################################################################

    # Criação de uma lista diária (dias corridos) com as datas desde o início do mês da primeira data de accrual até o fim do mês da última data de evento
    dt_min = min(mtm['dt_inicio'])
    dt_min = dt_min.replace(day=1)
    dt_min = datetime.date(dt_min.year, dt_min.month, dt_min.day)

    dt_max = max(mtm['dt_ref1'])
    dt_max = dt_max.replace(day=1, month=dt_max.month)
    dt_max = dt_max + pd.DateOffset(months=1)
    dt_max = dt_max - pd.DateOffset(days=1)
    dt_max = datetime.date(dt_max.year, dt_max.month, dt_max.day)

    dt_ref = pd.date_range(start=dt_min, end=dt_max, freq='D').date
    ano = pd.date_range(start=dt_min, end=dt_max, freq='D').year.astype(int)
    mes = pd.date_range(start=dt_min, end=dt_max, freq='D').month.astype(int)
    dias = pd.date_range(start=dt_min, end=dt_max, freq='D').day.astype(int)
    serie_dias = pd.DataFrame(columns=['dt_ref', 'ano', 'mes', 'dia'])
    serie_dias['dt_ref'] = dt_ref
    serie_dias['ano'] = ano
    serie_dias['mes'] = mes
    serie_dias['dia'] = dias

    ###############################################################################
    ##Criação de uma lista diária (dias uteis) com as datas desde o início do mês da primeira data de accrual até o fim do mês da última data de eventodt_max = max(serie_dias['dt_ref'])
    per = FinDt.DatasFinanceiras(dt_min, dt_max, path_arquivo=depend_path_feriados_nacionais)

    du = pd.DataFrame(columns=['dt_ref'])
    du['dt_ref'] = per.dias(3)
    du['du_1'] = 1

    # Auxiliar para não ficar rodando o FinDt
    # pd.io.sql.to_sql(du, name='du', con=connection, if_exists='replace', flavor='mysql')

    # x = 'select * from projeto_inv.du'
    # du = pd.read_sql(x, con=connection)
    # del du['index']
    ###############################################################################

    # Unifica as tabelas de dias úteis e dias corridos
    # --du['du_1'] = 1 -> DU
    # --du['du_1'] = 0 -> DC
    # --du['dc_1'] = 1 -> DC
    serie_dias = serie_dias.merge(du, on=['dt_ref'], how='left')
    # Preenche as linhas que não são dia útil com flag = 0 para dia útil
    serie_dias['du_1'] = serie_dias['du_1'].fillna(0)
    # Dias corridos são sempre 1
    serie_dias['dc_1'] = 1

    # Cálculo do número de dias corridos em cada mês
    # --Conta o número de datas na lista dt_ref, diferenciando por mês e ano
    temp = serie_dias[['dt_ref', 'ano', 'mes']].groupby(['ano', 'mes']).agg(['count'])
    temp = temp.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    temp1 = pd.DataFrame(columns=['ano', 'mes', 'dc'])
    temp1['ano'] = temp['ano']
    temp1['mes'] = temp['mes']
    temp1['dc'] = temp['dt_ref']

    serie_dias = serie_dias.merge(temp1, on=['ano', 'mes'], how='left')
    del temp, temp1

    # Cálculo do número de dias úteis em cada mês
    # --Soma onde tem o flag de día util = 1, somente quando mês e ano forem iguais
    temp = serie_dias[['du_1', 'ano', 'mes']].groupby(['ano', 'mes']).agg(['sum'])
    temp = temp.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    temp1 = pd.DataFrame(columns=['ano', 'mes', 'du'])
    temp1['ano'] = temp['ano']
    temp1['mes'] = temp['mes']
    temp1['du'] = temp['du_1']

    serie_dias = serie_dias.merge(temp1, on=['ano', 'mes'], how='left')
    del temp, temp1

    # Criação da série de índice

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    query = 'SELECT * FROM projeto_inv.bc_series'
    bc_series = pd.read_sql(query, con=connection)

    query = 'SELECT * FROM projeto_inv.anbima_projecao_indices'
    proj = pd.read_sql(query, con=connection)

    connection.close()

    proj['ano'] = proj['data_coleta'].astype(str).str[0:4]
    proj['mes'] = proj['data_coleta'].astype(str).str[5:7]

    ano = dtbase_concat[0:4]
    mes = dtbase_concat[4:6]

    proj = proj[(proj.ano == ano) & (proj.mes == mes) & (proj.mes_previsao == 'Corrente')].copy()

    proj = proj.sort(['data_coleta', 'data_bd'], ascending=[True, True])
    proj = proj.drop_duplicates(['data_coleta'], take_last=True)

    proj_ipca = proj[proj.indice.isin(['IPCA'])].copy()
    proj_ipca = proj_ipca[proj_ipca.data_coleta == max(proj_ipca.data_coleta)].copy()

    proj_igpm = proj[proj.indice.isin(['IGPM'])].copy()
    proj_igpm = proj_igpm[proj_igpm.data_coleta == max(proj_igpm.data_coleta)].copy()

    proj = proj_ipca.append(proj_igpm)

    del proj['id_anbima_projecao_indices']
    del proj['mes_coleta']
    del proj['mes_previsao']
    del proj['data_validade']
    del proj['data_bd']
    del proj['data_coleta']

    proj['indice'] = np.where(proj['indice'].isin(['IGPM']), 'IGP', proj['indice'])

    proj['ano'] = proj['ano'].astype(int)
    proj['mes'] = proj['mes'].astype(int)

    bc_series = bc_series.merge(proj, on=['indice', 'ano', 'mes'], how='left')

    bc_series['valor'] = np.where(bc_series['projecao'].notnull(), bc_series['projecao'], bc_series['valor'])

    del bc_series['du_1']
    del bc_series['dc_1']
    del bc_series['dc']
    del bc_series['du']
    del bc_series['fator_dia_dc']
    del bc_series['fator_dia_du']
    del bc_series['fator_acum_dc']
    del bc_series['fator_acum_du']

    bc_series_virgem = bc_series.copy()

    bc_series = bc_series_virgem.sort(['indice', 'ano', 'mes', 'dia', 'data_bd'], ascending=[True, True, True, True, False])
    bc_series = bc_series.drop_duplicates(subset=['dt_ref', 'ano', 'mes', 'dia', 'indice'], take_last=False)

    del bc_series['data_bd']
    del bc_series['dt_ref']
    del bc_series['id_bc_series']
    del bc_series['projecao']

    bc_series = bc_series.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################


    # Como estamos calculando numa data que não há proj, usamos o observado. Depois da data base de cálculo, não atualiza
    bc_series['valor'] = bc_series['valor'].fillna(0)
    ###############################################################################

    bc_series['valor'] = bc_series['valor'].astype(float)

    # Separa os indexadores em séries diferentes
    series_ipca = bc_series[bc_series['indice'].isin(['IPCA'])]
    series_igpm = bc_series[bc_series['indice'].isin(['IGP'])]
    series_tjlp = bc_series[bc_series['indice'].isin(['TJLP'])]
    series_cdi = bc_series[bc_series['indice'].isin(['DI1'])]

    # IPCA
    series_ipca = series_ipca.merge(serie_dias, on=['ano', 'mes', 'dia'], how='right')
    series_ipca['indice'] = series_ipca['indice'].fillna('IPCA')
    series_ipca['valor'] = series_ipca['valor'].fillna(0)
    series_ipca['tx_dia_du'] = (1 + series_ipca.du_1 * series_ipca.valor / 100) ** (series_ipca.du_1 / series_ipca.du)
    series_ipca['tx_dia_dc'] = (1 + series_ipca.dc_1 * series_ipca.valor / 100) ** (series_ipca.dc_1 / series_ipca.dc)
    series_ipca['tx_acum_du'] = np.cumprod(series_ipca.tx_dia_du)
    series_ipca['tx_acum_dc'] = np.cumprod(series_ipca.tx_dia_dc)
    series_ipca['indice_dc'] = np.cumsum(series_ipca['dc_1'])
    series_ipca['indice_du'] = np.cumsum(series_ipca['du_1'])

    # series_ipca.to_excel(save_path+'/ipca_mtm.xlsx')

    # IGP-M
    series_igpm = series_igpm.merge(serie_dias, on=['ano', 'mes', 'dia'], how='right')
    series_igpm['indice'] = series_igpm['indice'].fillna('IGP')
    series_igpm['valor'] = series_igpm['valor'].fillna(0)
    series_igpm['tx_dia_du'] = (1 + series_igpm.du_1 * series_igpm.valor / 100) ** (series_igpm.du_1 / series_igpm.du)
    series_igpm['tx_dia_dc'] = (1 + series_igpm.dc_1 * series_igpm.valor / 100) ** (series_igpm.dc_1 / series_igpm.dc)
    series_igpm['tx_acum_du'] = np.cumprod(series_igpm.tx_dia_du)
    series_igpm['tx_acum_dc'] = np.cumprod(series_igpm.tx_dia_dc)
    series_igpm['indice_dc'] = np.cumsum(series_igpm['dc_1'])
    series_igpm['indice_du'] = np.cumsum(series_igpm['du_1'])

    # TJLP
    series_tjlp = series_tjlp.merge(serie_dias, on=['ano', 'mes', 'dia'], how='right')
    series_tjlp['indice'] = series_tjlp['indice'].fillna('TJLP')
    series_tjlp['valor'] = series_tjlp['valor'].fillna(0)
    series_tjlp['tx_dia_du'] = (1 + series_tjlp.du_1 * series_tjlp.valor / 100) ** (series_tjlp.du_1 / series_tjlp.du)
    series_tjlp['tx_dia_dc'] = (1 + series_tjlp.dc_1 * series_tjlp.valor / 100) ** (series_tjlp.dc_1 / series_tjlp.dc)
    series_tjlp['tx_acum_du'] = np.cumprod(series_tjlp.tx_dia_du)
    series_tjlp['tx_acum_dc'] = np.cumprod(series_tjlp.tx_dia_dc)
    series_tjlp['indice_dc'] = np.cumsum(series_tjlp['dc_1'])
    series_tjlp['indice_du'] = np.cumsum(series_tjlp['du_1'])

    # CDI
    series_cdi = series_cdi.merge(serie_dias, on=['ano', 'mes', 'dia'], how='right')
    series_cdi['indice'] = series_cdi['indice'].fillna('DI1')
    series_cdi['valor'] = series_cdi['valor'].fillna(0)
    series_cdi['tx_dia_du'] = ((1 + series_cdi.du_1 * series_cdi.valor / 100) ** (series_cdi.du_1 / 252))
    series_cdi['tx_dia_dc'] = 0
    series_cdi['tx_acum_du'] = np.cumprod(series_cdi.tx_dia_du)
    series_cdi['tx_acum_dc'] = 0
    series_cdi['indice_dc'] = 0
    series_cdi['indice_du'] = np.cumsum(series_cdi['du_1'])

    # series_cdi.to_excel(save_path+'/cdi_mtm.xlsx')

    # PRE
    series_pre = series_ipca.copy()
    series_pre['tx_acum_du'] = 1
    series_pre['tx_acum_dc'] = 1
    series_pre['indice'] = 'PRE'

    # Une todas as series
    series_fim = series_ipca
    series_fim = series_fim.append(series_igpm)
    series_fim = series_fim.append(series_cdi)
    series_fim = series_fim.append(series_pre)
    series_fim = series_fim.append(series_tjlp)

    # Atualiza o nome dos índices
    series_fim['indexador'] = series_fim['indice']
    series_fim = series_fim.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # Salva um excel com a sequencia dos indices e taxas
    # series_fim.to_excel(save_path+'/series.xlsx')

    ###############################################################################
    # 6 - União da base de indexadores com a base de mtm
    ###############################################################################

    inicio = series_fim.copy()
    del inicio['du_1']
    del inicio['dc_1']
    del inicio['valor']
    del inicio['indice']
    # del inicio['data_bd']
    del inicio['tx_dia_du']
    del inicio['tx_dia_dc']
    del inicio['du']
    del inicio['dc']
    del inicio['dt_ref']
    del inicio['flag']

    ###############################################################################
    # Aparentemente, não há esta coluna - só pq eu não rodei o esquema...
    # del inicio['id_bc_series']
    ###############################################################################

    fim = inicio.copy()

    inicio = inicio.rename(
        columns={'ano': 'ano_inicio', 'mes': 'mes_inicio', 'dia': 'dia_inicio', 'tx_acum_du': 'tx_acum_du_inicio',
                 'tx_acum_dc': 'tx_acum_dc_inicio', 'indice_dc': 'indice_dc_inicio', 'indice_du': 'indice_du_inicio'})
    fim = fim.rename(columns={'ano': 'ano_fim', 'mes': 'mes_fim', 'dia': 'dia_fim', 'tx_acum_du': 'tx_acum_du_fim',
                              'tx_acum_dc': 'tx_acum_dc_fim', 'indice_dc': 'indice_dc_fim', 'indice_du': 'indice_du_fim'})

    mtm = mtm.merge(inicio, on=['ano_inicio', 'mes_inicio', 'dia_inicio', 'indexador'], how='left')
    mtm = mtm.merge(fim, on=['ano_fim', 'mes_fim', 'dia_fim', 'indexador'], how='left')

    mtm = mtm.sort(
        ['codigo_isin', 'codigo_cetip', 'tipo_ativo', 'data_emissao', 'data_expiracao', 'valor_nominal', 'indexador',
         'percentual_indexador', 'cod_frequencia_juros', 'data_primeiro_pagamento_juros', 'id_bmf_numeraca', 'data_bd'],
        ascending=[True, True, True, True, True, True, True, True, True, True, True, True])
    mtm = mtm.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################
    # 7.0 - Criação do arquivo erros de divisão
    ###############################################################################
    writer = ExcelWriter(save_path_erros)

    ###############################################################################
    # 7 - Inicialização das contas
    ###############################################################################

    # --Just in case...
    mtm['juros_dc_du'] = mtm['juros_dc_du'].astype(int)
    mtm['indexador_dc_du'] = mtm['indexador_dc_du'].astype(int)

    ###############################################################################
    # Taxa acumulada inicio vazia
    erros = mtm[(mtm.tx_acum_du_inicio.isnull()) | (mtm.tx_acum_du_inicio == 0)]
    erros.to_excel(writer, 'tx_acum_du_inicio')
    mtm = mtm[(mtm.tx_acum_du_inicio.notnull()) | (mtm.tx_acum_du_inicio != 0)]

    # Taxa acumulada fim vazia
    erros = mtm[(mtm.tx_acum_du_fim.isnull()) | (mtm.tx_acum_du_fim == 0)]
    erros.to_excel(writer, 'tx_acum_du_fim')
    mtm = mtm[(mtm.tx_acum_du_fim.notnull()) | (mtm.tx_acum_du_fim != 0)]
    ###############################################################################

    # --Taxa acumulada
    mtm['fator_index_per'] = 0
    mtm['indexador_dc_du'] = mtm['indexador_dc_du'].fillna(252)
    mtm['fator_index_per'][mtm['indexador_dc_du'] == 252] = mtm['tx_acum_du_fim'][mtm['indexador_dc_du'] == 252] / \
                                                            mtm['tx_acum_du_inicio'][mtm['indexador_dc_du'] == 252]
    mtm['fator_index_per'][mtm['indexador_dc_du'] == 360] = mtm['tx_acum_du_fim'][mtm['indexador_dc_du'] == 360] / \
                                                            mtm['tx_acum_du_inicio'][mtm['indexador_dc_du'] == 360]
    # --Número de dias entre as datas de inicio e fim
    mtm['du_per'] = mtm['indice_du_fim'] - mtm['indice_du_inicio']
    mtm['dc_per'] = mtm['indice_dc_fim'] - mtm['indice_dc_inicio']
    # --Preenche as taxas de juros
    mtm['taxa_juros'] = mtm['taxa_juros'].fillna(0)
    mtm['taxa_juros'] = mtm['taxa_juros'].astype(float)

    # --Calcula o fator de juros em cada uma das modalidades
    mtm['fator_juros_per_du_exp'] = (1 + mtm['taxa_juros'] / 100) ** (mtm['du_per'] / 252)
    mtm['fator_juros_per_dc_exp'] = (1 + mtm['taxa_juros'] / 100) ** (mtm['dc_per'] / 360)
    mtm['fator_juros_per_du_lin'] = 1 + mtm['taxa_juros'] / 100 * (mtm['du_per'] / 252)
    mtm['fator_juros_per_dc_lin'] = 1 + mtm['taxa_juros'] / 100 * (mtm['dc_per'] / 360)
    mtm['juros_dc_du'] = mtm['juros_dc_du'].fillna(252)
    mtm['fator_juros_per'] = 0
    mtm['fator_juros_per'][(mtm['tipo_capitalizacao'] == 'Exponencial') & (mtm['juros_dc_du'] == 252)] = \
    mtm['fator_juros_per_du_exp'][(mtm['tipo_capitalizacao'] == 'Exponencial') & (mtm['juros_dc_du'] == 252)]
    mtm['fator_juros_per'][(mtm['tipo_capitalizacao'] == 'Exponencial') & (mtm['juros_dc_du'] == 360)] = \
    mtm['fator_juros_per_dc_exp'][(mtm['tipo_capitalizacao'] == 'Exponencial') & (mtm['juros_dc_du'] == 360)]
    mtm['fator_juros_per'][(mtm['tipo_capitalizacao'] == 'Linear') & (mtm['juros_dc_du'] == 252)] = \
    mtm['fator_juros_per_du_lin'][(mtm['tipo_capitalizacao'] == 'Linear') & (mtm['juros_dc_du'] == 252)]
    mtm['fator_juros_per'][(mtm['tipo_capitalizacao'] == 'Linear') & (mtm['juros_dc_du'] == 360)] = \
    mtm['fator_juros_per_dc_lin'][(mtm['tipo_capitalizacao'] == 'Linear') & (mtm['juros_dc_du'] == 360)]

    mtm['valor_nominal'] = mtm['valor_nominal'].astype(float)
    mtm['vne'] = 0.0
    mtm.loc[pd.to_datetime(mtm['dt_inicio_rentab']) == pd.to_datetime(mtm['dt_inicio']), 'vne'] = mtm['valor_nominal']
    mtm['vna'] = mtm['vne']

    mtm['juros_per'] = 0.0
    mtm['saldo_dev_juros'] = 0.0
    mtm['pagto_juros'] = 0.0
    mtm['perc_amortizacao'] = mtm['perc_amortizacao'].astype(float)
    mtm['perc_amortizacao'] = mtm['perc_amortizacao'].fillna(0)
    mtm['vl_amortizacao'] = 0.0
    mtm['fv'] = 0.0
    mtm['juros_tipo1'] = np.where(mtm['juros_tipo'] == "liquidate", 1, 0)
    mtm['dt_ref2'] = mtm['dt_ref1'].dt.date
    mtm['fator_juros_per'] = mtm['fator_juros_per'].fillna(0)
    mtm['percentual_indexador'] = mtm['percentual_indexador'].fillna(100)

    # Linha de pagamento final
    mtm['codigo_isin_final'] = mtm['codigo_isin'].shift(-1)

    ###############################################################################
    # 8 - Papéis CDI - Err ~ 0,00001%
    ###############################################################################

    mtm_aux = mtm[mtm['indexador'] == 'DI1'].copy()
    mtm_aux = mtm_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################

    # Gera novamente a coluna de codigo_isin_lag já que agora há uma nova lista
    mtm_aux['codigo_isin_lag'] = mtm_aux['codigo_isin'].shift(1)
    # Cria colunas fator indexador acumulado
    # --Não são necessárias para papéis CDI - Não há atualização de valor nominal
    mtm_aux['fator_index_acum'] = mtm_aux[['codigo_isin', 'fator_index_per']].groupby(['codigo_isin']).agg(['cumprod'])
    mtm_aux['fator_index_acum_lag'] = mtm_aux['fator_index_acum'].shift(1)

    mtm_aux['pagto_amortizacao'] = 0
    mtm_aux['pagto_amortizacao'][mtm_aux['perc_amortizacao'] != 0] = 1
    mtm_aux['pagto_amortizacao'] = mtm_aux['pagto_amortizacao'].fillna(0)

    x = mtm_aux[['codigo_isin', 'perc_amortizacao', 'pagto_amortizacao']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'num_parc_amt', 'total_amortizacao'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['num_parc_amt'] = x['pagto_amortizacao']
    x1['total_amortizacao'] = x['perc_amortizacao']
    mtm_aux = mtm_aux.merge(x1, on=['codigo_isin'], how='left')

    # Seleciona as debentures que cujo percentual de amortizacao termina em 100% e não soma 100% -> fluxo manual
    mtm_aux['perc_amortizacao1'] = mtm_aux['perc_amortizacao']
    mtm_aux['perc_amortizacao'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)] = 1 / mtm_aux[
        'num_parc_amt'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)]

    mtm_aux['perc_amortizacao'][(mtm_aux.total_amortizacao == 0) & (mtm_aux.codigo_isin_final != mtm_aux.codigo_isin)] = 1

    # Will, ressalta na documentação esse isin manual aqui
    # Tem que colocar aqui todos os isins que tem fluxo manual e que tem percentual de amortização não estático
    mtm_aux['perc_amortizacao'] = np.where(mtm_aux['id_papel'].isin(['BRMGIPDBS000']), mtm_aux['perc_amortizacao1'] / 100,
                                           mtm_aux['perc_amortizacao'])

    mtm_aux['perc_amortizacao_acum'] = mtm_aux[['codigo_isin', 'perc_amortizacao']].groupby(['codigo_isin']).agg(['cumsum'])
    mtm_aux['perc_amortizacao_acum_lag'] = mtm_aux['perc_amortizacao_acum'].shift(1)
    mtm_aux['perc_amortizacao_acum_dt'] = mtm_aux['perc_amortizacao_acum'] * mtm_aux['pagto_amortizacao']
    mtm_aux['perc_amortizacao_acum_dt'][mtm_aux.codigo_isin_final != mtm_aux.codigo_isin] = 1

    del mtm_aux['perc_amortizacao1']
    # Cálculo do FatorDI
    cdi_inicio = series_cdi.copy()

    del cdi_inicio['dt_ref']
    del cdi_inicio['valor']
    del cdi_inicio['indice']
    # del cdi_inicio['data_bd']
    del cdi_inicio['du_1']
    del cdi_inicio['dc_1']
    del cdi_inicio['du']
    del cdi_inicio['dc']
    del cdi_inicio['tx_dia_du']
    del cdi_inicio['tx_dia_dc']
    del cdi_inicio['tx_acum_du']
    del cdi_inicio['tx_acum_dc']
    del cdi_inicio['indice_dc']
    del cdi_inicio['indice_du']

    cdi_fim = cdi_inicio.copy()

    cdi_inicio = cdi_inicio.rename(columns={'ano': 'ano_inicio', 'mes': 'mes_inicio', 'dia': 'dia_inicio'})
    cdi_fim = cdi_fim.rename(
        columns={'ano': 'ano_fim', 'mes': 'mes_fim', 'dia': 'dia_fim', 'tx_acum_fator_di_inicio': 'tx_acum_fator_di_fim'})

    idx = mtm_aux[mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']].index.tolist()

    mtm_aux['tx_acum_fator_di_inicio'] = 0
    mtm_aux['tx_acum_fator_di_fim'] = 0

    for i in idx:
        mtm_aux_inicio = mtm_aux[['codigo_isin', 'ano_inicio', 'mes_inicio', 'dia_inicio']]
        mtm_aux_fim = mtm_aux[['codigo_isin', 'ano_fim', 'mes_fim', 'dia_fim']]

        cdi_inicio['codigo_isin'] = mtm_aux['codigo_isin'][i]
        aux = 1 + (series_cdi['tx_dia_du'] - 1) * mtm_aux['percentual_indexador'][i] / 100
        cdi_inicio['tx_acum_fator_di_inicio_aux'] = np.cumprod(aux)
        mtm_aux_inicio = mtm_aux_inicio.merge(cdi_inicio, on=['codigo_isin', 'ano_inicio', 'mes_inicio', 'dia_inicio'],
                                              how='left')
        mtm_aux_inicio = mtm_aux_inicio.where((pd.notnull(mtm_aux_inicio)), 0)

        cdi_fim['codigo_isin'] = mtm_aux['codigo_isin'][i]
        aux = 1 + (series_cdi['tx_dia_du'] - 1) * mtm_aux['percentual_indexador'][i] / 100
        cdi_fim['tx_acum_fator_di_fim_aux'] = np.cumprod(aux)
        mtm_aux_fim = mtm_aux_fim.merge(cdi_fim, on=['codigo_isin', 'ano_fim', 'mes_fim', 'dia_fim'], how='left')
        mtm_aux_fim = mtm_aux_fim.where((pd.notnull(mtm_aux_fim)), 0)

        mtm_aux['tx_acum_fator_di_inicio'][mtm_aux.codigo_isin == mtm_aux['codigo_isin'][i]] = \
        mtm_aux_inicio['tx_acum_fator_di_inicio_aux'][mtm_aux_inicio['tx_acum_fator_di_inicio_aux'].notnull()].copy()
        mtm_aux['tx_acum_fator_di_fim'][mtm_aux.codigo_isin == mtm_aux['codigo_isin'][i]] = \
        mtm_aux_fim['tx_acum_fator_di_fim_aux'][mtm_aux_fim['tx_acum_fator_di_fim_aux'].notnull()].copy()

        del mtm_aux_inicio['tx_acum_fator_di_inicio_aux']
        del mtm_aux_fim['tx_acum_fator_di_fim_aux']

    mtm_aux['vne'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
        mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']]
    mtm_aux['vne'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']] * (
                                                                           1 - mtm_aux['perc_amortizacao_acum_lag'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']])
    mtm_aux['vna'] = mtm_aux['vne']
    mtm_aux['vl_amortizacao'] = mtm_aux['valor_nominal'] * mtm_aux['perc_amortizacao']

    # Calcula os juros na primeira data de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']].index.tolist()

    ###############################################################################
    # Taxa acumulada inicio vazia
    erros = mtm_aux[(mtm_aux.tx_acum_fator_di_inicio.isnull()) | (mtm_aux.tx_acum_fator_di_inicio == 0)]
    erros.to_excel(writer, 'tx_acum_fator_di_inicio')
    mtm_aux = mtm_aux[(mtm_aux.tx_acum_du_inicio.notnull()) | (mtm_aux.tx_acum_du_inicio != 0)]

    # Taxa acumulada fim vazia
    erros = mtm_aux[(mtm_aux.tx_acum_fator_di_fim.isnull()) | (mtm_aux.tx_acum_fator_di_fim == 0)]
    erros.to_excel(writer, 'tx_acum_fator_di_fim')
    mtm_aux = mtm_aux[(mtm_aux.tx_acum_du_fim.notnull()) | (mtm_aux.tx_acum_du_fim != 0)]
    ###############################################################################

    mtm_aux['fator_index_per'] = mtm_aux['tx_acum_fator_di_fim'] / mtm_aux['tx_acum_fator_di_inicio']

    mtm_aux['juros_per'][idx] = mtm_aux['vne'][idx] * (
    mtm_aux['fator_juros_per'][idx] * mtm_aux['tx_acum_fator_di_fim'][idx] / mtm_aux['tx_acum_fator_di_inicio'][idx] - 1)
    mtm_aux['pagto_juros'][idx] = mtm_aux['juros_per'][idx] * mtm_aux['juros_tipo1'][idx] * mtm_aux['percentual_juros'][idx]
    mtm_aux['saldo_dev_juros'][idx] = mtm_aux['juros_per'][idx] - mtm_aux['pagto_juros'][idx]

    # Calcula os juros nas outras datas de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']].index.tolist()
    i = 1
    for i in idx:
        mtm_aux['juros_per'][i] = (mtm_aux['vne'][i] + mtm_aux['saldo_dev_juros'][i - 1]) * (
        mtm_aux['fator_juros_per'][i] * mtm_aux['tx_acum_fator_di_fim'][i] / mtm_aux['tx_acum_fator_di_inicio'][i] - 1) + \
                                  mtm_aux['saldo_dev_juros'][i - 1]
        mtm_aux['pagto_juros'][i] = mtm_aux['juros_per'][i] * mtm_aux['juros_tipo1'][i] * mtm_aux['percentual_juros'][i]
        mtm_aux['saldo_dev_juros'][i] = mtm_aux['juros_per'][i] - mtm_aux['pagto_juros'][i]

    mtm_aux['valor_nominal_data'] = mtm_aux['vna'] + mtm_aux['saldo_dev_juros'] - mtm_aux['vl_amortizacao']
    mtm_aux['fv'] = mtm_aux['pagto_juros'] + mtm_aux['vl_amortizacao']

    mtm_aux['principal_perc'] = mtm_aux['vl_amortizacao'] / mtm_aux['vne']
    mtm_aux['pagto_juros_perc'] = mtm_aux['pagto_juros'] / mtm_aux['vne']
    mtm_aux['saldo_dev_juros_perc'] = mtm_aux['saldo_dev_juros'] / mtm_aux['vna']

    mtm_aux_todos = mtm_aux.copy()

    ###############################################################################
    # 9 - Papéis amortizados via VNE
    ###############################################################################

    mtm_aux = mtm[(mtm['tipo'] == 'vne') & (mtm['indexador'] != 'DI1')].copy()
    mtm_aux = mtm_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################

    # Gera novamente a coluna de codigo_isin_lag já que agora há uma nova lista
    mtm_aux['codigo_isin_lag'] = mtm_aux['codigo_isin'].shift(1)
    # Cria colunas LAG - fator indexador acumulado
    mtm_aux['fator_index_acum'] = mtm_aux[['codigo_isin', 'fator_index_per']].groupby(['codigo_isin']).agg(['cumprod'])
    mtm_aux['fator_index_acum_lag'] = mtm_aux['fator_index_acum'].shift(1)

    mtm_aux['pagto_amortizacao'] = 0
    mtm_aux['pagto_amortizacao'][mtm_aux['perc_amortizacao'] != 0] = 1
    mtm_aux['pagto_amortizacao'] = mtm_aux['pagto_amortizacao'].fillna(0)

    x = mtm_aux[['codigo_isin', 'perc_amortizacao', 'pagto_amortizacao']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'num_parc_amt', 'total_amortizacao'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['num_parc_amt'] = x['pagto_amortizacao']
    x1['total_amortizacao'] = x['perc_amortizacao']
    mtm_aux = mtm_aux.merge(x1, on=['codigo_isin'], how='left')
    mtm_aux['perc_amortizacao1'] = mtm_aux['perc_amortizacao']
    mtm_aux['perc_amortizacao'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)] = 1 / mtm_aux[
        'num_parc_amt'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)]

    mtm_aux['perc_amortizacao_acum'] = mtm_aux[['codigo_isin', 'perc_amortizacao']].groupby(['codigo_isin']).agg(['cumsum'])
    mtm_aux['perc_amortizacao_acum_lag'] = mtm_aux['perc_amortizacao_acum'].shift(1)
    mtm_aux['perc_amortizacao_acum_lag'] = mtm_aux['perc_amortizacao_acum_lag'].fillna(0)
    mtm_aux['perc_amortizacao_acum_dt'] = mtm_aux['perc_amortizacao_acum'] * mtm_aux['pagto_amortizacao']
    mtm_aux['perc_amortizacao_atualizado'] = mtm_aux['perc_amortizacao'] / (1 - mtm_aux['perc_amortizacao_acum_lag'])
    mtm_aux['perc_amortizacao_atualizado'][mtm_aux.codigo_isin_final != mtm_aux.codigo_isin] = 1
    mtm_aux['vne'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
        mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']]
    mtm_aux['vne'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']] * \
                                                                           mtm_aux['fator_index_acum_lag'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']] * (
                                                                           1 - mtm_aux['perc_amortizacao_acum_lag'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']])
    mtm_aux['vna'] = mtm_aux['vne'] * mtm_aux['fator_index_per']
    mtm_aux['vl_amortizacao'] = mtm_aux['vna'] * mtm_aux['perc_amortizacao_atualizado']

    del mtm_aux['perc_amortizacao1']

    # Calcula os juros na primeira data de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']].index.tolist()

    mtm_aux['juros_per'][idx] = mtm_aux['vne'][idx] * mtm_aux['fator_index_per'][idx] * (
    mtm_aux['fator_juros_per'][idx] - 1)
    mtm_aux['pagto_juros'][idx] = mtm_aux['juros_per'][idx] * mtm_aux['juros_tipo1'][idx] * mtm_aux['percentual_juros'][idx]
    mtm_aux['saldo_dev_juros'][idx] = mtm_aux['juros_per'][idx] - mtm_aux['pagto_juros'][idx]

    # Calcula os juros nas outras datas de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']].index.tolist()

    for i in idx:
        mtm_aux['juros_per'][i] = (mtm_aux['vne'][i] + mtm_aux['saldo_dev_juros'][i - 1]) * mtm_aux['fator_index_per'][
            i] * (mtm_aux['fator_juros_per'][i] - 1) + mtm_aux['saldo_dev_juros'][i - 1]
        mtm_aux['pagto_juros'][i] = mtm_aux['juros_per'][i] * mtm_aux['juros_tipo1'][i] * mtm_aux['percentual_juros'][i]
        mtm_aux['saldo_dev_juros'][i] = mtm_aux['juros_per'][i] - mtm_aux['pagto_juros'][i]

    mtm_aux['valor_nominal_data'] = mtm_aux['vna'] + mtm_aux['saldo_dev_juros'] - mtm_aux['vl_amortizacao']
    mtm_aux['fv'] = mtm_aux['pagto_juros'] + mtm_aux['vl_amortizacao']

    # mtm_aux.to_excel('C:/Users/Cora.Santos/Desktop/HDI-Investimentos/mtm_IPCA-6meses.xlsx')

    mtm_aux['principal_perc'] = mtm_aux['vl_amortizacao'] / mtm_aux['vne']
    mtm_aux['pagto_juros_perc'] = mtm_aux['pagto_juros'] / mtm_aux['vne']
    mtm_aux['saldo_dev_juros_perc'] = mtm_aux['saldo_dev_juros'] / mtm_aux['vna']

    mtm_aux_todos = mtm_aux_todos.append(mtm_aux)

    ###############################################################################
    # 10 - Papéis amortizados via VNA
    ###############################################################################

    mtm_aux = mtm[(mtm['tipo'] == 'vna') & (mtm['indexador'] != 'DI1')].copy()
    mtm_aux = mtm_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # Gera novamente a coluna de codigo_isin_lag já que agora há uma nova lista
    mtm_aux['codigo_isin_lag'] = mtm_aux['codigo_isin'].shift(1)
    # Cria colunas LAG - fator indexador acumulado
    mtm_aux['fator_index_acum'] = mtm_aux[['codigo_isin', 'fator_index_per']].groupby(['codigo_isin']).agg(['cumprod'])
    mtm_aux['fator_index_acum_lag'] = mtm_aux['fator_index_acum'].shift(1)

    mtm_aux['perc_amortizacao1'] = mtm_aux['perc_amortizacao']
    mtm_aux['pagto_amortizacao'] = 0
    mtm_aux['pagto_amortizacao'][mtm_aux['perc_amortizacao'] != 0] = 1
    mtm_aux['pagto_amortizacao'] = mtm_aux['pagto_amortizacao'].fillna(0)

    x = mtm_aux[['codigo_isin', 'perc_amortizacao', 'pagto_amortizacao']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'num_parc_amt', 'total_amortizacao'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['num_parc_amt'] = x['pagto_amortizacao']
    x1['total_amortizacao'] = x['perc_amortizacao']
    mtm_aux = mtm_aux.merge(x1, on=['codigo_isin'], how='left')
    mtm_aux['perc_amortizacao'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)] = 1 / mtm_aux[
        'num_parc_amt'][(mtm_aux.total_amortizacao != 0) & (mtm_aux.perc_amortizacao != 0)]
    mtm_aux['perc_amortizacao'] = np.where(mtm_aux['flag'].isin(['FM1']), mtm_aux['perc_amortizacao1'],
                                           mtm_aux['perc_amortizacao'])

    mtm_aux['perc_amortizacao_acum'] = 0
    mtm_aux['perc_amortizacao_acum_lag'] = 0
    mtm_aux['perc_amortizacao_acum'] = mtm_aux[['codigo_isin', 'perc_amortizacao']].groupby(['codigo_isin']).agg(['cumsum'])
    mtm_aux['perc_amortizacao_acum_lag'] = mtm_aux['perc_amortizacao_acum'].shift(1)
    mtm_aux['perc_amortizacao_atualizado'] = mtm_aux['perc_amortizacao']
    mtm_aux['perc_amortizacao_atualizado'][mtm_aux.codigo_isin == mtm_aux.codigo_isin_lag] = mtm_aux['perc_amortizacao'][
                                                                                                 mtm_aux.codigo_isin == mtm_aux.codigo_isin_lag] / (
                                                                                             1 - mtm_aux[
                                                                                                 'perc_amortizacao_acum_lag'][
                                                                                                 mtm_aux.codigo_isin == mtm_aux.codigo_isin_lag])

    mtm_aux['perc_amortizacao_atualizado'][mtm_aux.codigo_isin_final != mtm_aux.codigo_isin] = 1
    mtm_aux['fator_amortizacao_acum_dt'] = 1 - mtm_aux['perc_amortizacao_atualizado']
    mtm_aux['fator_amortizacao_acum_dt'] = mtm_aux[['codigo_isin', 'fator_amortizacao_acum_dt']].groupby(
        ['codigo_isin']).agg(['cumprod'])
    mtm_aux['fator_amortizacao_acum_dt_lag'] = mtm_aux['fator_amortizacao_acum_dt'].shift(1)
    mtm_aux['fator_amortizacao_acum_dt_lag'] = mtm_aux['fator_amortizacao_acum_dt_lag'].fillna(1)
    mtm_aux['vne'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
        mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']]
    mtm_aux['vne'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']] = mtm_aux['valor_nominal'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']] * \
                                                                           mtm_aux['fator_index_acum_lag'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']] * \
                                                                           mtm_aux['fator_amortizacao_acum_dt_lag'][
                                                                               mtm_aux['codigo_isin'] == mtm_aux[
                                                                                   'codigo_isin_lag']]
    mtm_aux['vna'] = mtm_aux['vne'] * mtm_aux['fator_index_per']
    mtm_aux['vl_amortizacao'] = mtm_aux['vna'] * mtm_aux['perc_amortizacao_atualizado']

    aux = mtm_aux[['tipo', 'dt_fim', 'data_primeiro_pagamento_juros', 'perc_amortizacao_acum_lag',
                   'perc_amortizacao_atualizado']].copy()

    # Calcula os juros na primeira data de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] != mtm_aux['codigo_isin_lag']].index.tolist()

    mtm_aux['juros_per'][idx] = mtm_aux['vne'][idx] * mtm_aux['fator_index_per'][idx] * (
    mtm_aux['fator_juros_per'][idx] - 1)
    mtm_aux['pagto_juros'][idx] = mtm_aux['juros_per'][idx] * mtm_aux['juros_tipo1'][idx] * mtm_aux['percentual_juros'][idx]
    mtm_aux['saldo_dev_juros'][idx] = mtm_aux['juros_per'][idx] - mtm_aux['pagto_juros'][idx]

    # Calcula os juros nas outras datas de evento financeiro
    idx = mtm_aux['valor_nominal'][mtm_aux['codigo_isin'] == mtm_aux['codigo_isin_lag']].index.tolist()

    for i in idx:
        mtm_aux['juros_per'][i] = (mtm_aux['vne'][i] + mtm_aux['saldo_dev_juros'][i - 1]) * mtm_aux['fator_index_per'][
            i] * (mtm_aux['fator_juros_per'][i] - 1) + mtm_aux['saldo_dev_juros'][i - 1]
        mtm_aux['pagto_juros'][i] = mtm_aux['juros_per'][i] * mtm_aux['juros_tipo1'][i] * mtm_aux['percentual_juros'][i]
        mtm_aux['saldo_dev_juros'][i] = mtm_aux['juros_per'][i] - mtm_aux['pagto_juros'][i]

    mtm_aux['valor_nominal_data'] = mtm_aux['vna'] + mtm_aux['saldo_dev_juros'] - mtm_aux['vl_amortizacao']
    mtm_aux['fv'] = mtm_aux['pagto_juros'] + mtm_aux['vl_amortizacao']

    mtm_aux['principal_perc'] = mtm_aux['vl_amortizacao'] / mtm_aux['vne']
    mtm_aux['pagto_juros_perc'] = mtm_aux['pagto_juros'] / mtm_aux['vne']
    mtm_aux['saldo_dev_juros_perc'] = mtm_aux['saldo_dev_juros'] / mtm_aux['vna']

    mtm_aux_todos = mtm_aux_todos.append(mtm_aux)

    mtm_aux_todos = mtm_aux_todos.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################
    # 11 - Criação e preenchimento tabela FV
    ###############################################################################

    fv = pd.DataFrame(columns=['id_bmf_numeraca',
                               'id_anbima_debentures',
                               'data_bd',
                               'codigo_isin',
                               'codigo_cetip',
                               'tipo_ativo',
                               'data_emissao',
                               'data_expiracao',
                               'data_inicio_rent',
                               'data_primeiro_pagamento_juros',
                               'valor_nominal',
                               'indexador',
                               'indexador_dc_du',
                               'percentual_indexador',
                               'tipo_capitalizacao',
                               'juros_dc_du',
                               'juros_cada',
                               'juros_unidade',
                               'taxa_juros',
                               'tipo',
                               'data_ref',
                               'flag_inclusao',
                               'juros_tipo',
                               'indexador_tipo',
                               'data_inicio',
                               'ano_inicio',
                               'mes_inicio',
                               'dia_inicio',
                               'indice_du_inicio',
                               'indice_dc_inicio',
                               'data_fim',
                               'ano_fim',
                               'mes_fim',
                               'dia_fim',
                               'indice_du_fim',
                               'indice_dc_fim',
                               'du_per',
                               'dc_per',
                               'tx_acum_du_inicio',
                               'tx_acum_dc_inicio',
                               'tx_acum_du_fim',
                               'tx_acum_dc_fim',
                               'tx_acum_fator_di_inicio',
                               'tx_acum_fator_di_fim',
                               'fator_index_per',
                               'fator_index_acum',
                               'fator_index_acum_lag',
                               'perc_amortizacao',
                               'pagto_amortizacao',
                               'perc_amortizacao_acum',
                               'perc_amortizacao_acum_lag',
                               'perc_amortizacao_acum_dt',
                               'fator_amortizacao_acum_dt',
                               'fator_amortizacao_acum_dt_lag',
                               'vne',
                               'vna',
                               'vl_amortizacao',
                               'principal_perc',
                               'juros_tipo1',
                               'fator_juros_per_du_exp',
                               'fator_juros_per_dc_exp',
                               'fator_juros_per_du_lin',
                               'fator_juros_per_dc_lin',
                               'fator_juros_per',
                               'juros_per',
                               'percentual_juros',
                               'pagto_juros',
                               'saldo_dev_juros',
                               'pagto_juros_perc',
                               'saldo_dev_juros_perc',
                               'valor_nominal_data',
                               'fv',
                               ])

    # Auto increment na bd
    # fv['id_fv'] = mtm_aux_todos['']
    fv['id_bmf_numeraca'] = mtm_aux_todos['id_bmf_numeraca']
    fv['id_anbima_debentures'] = mtm_aux_todos['id_anbima_debentures']
    fv['id_papel'] = mtm_aux_todos['id_papel']
    fv['flag'] = mtm_aux_todos['flag']
    fv['data_bd'] = datetime.datetime.now()
    fv['codigo_isin'] = mtm_aux_todos['codigo_isin']
    fv['codigo_cetip'] = mtm_aux_todos['codigo_cetip']
    fv['tipo_ativo'] = mtm_aux_todos['tipo_ativo']
    fv['dtoperacao'] = mtm_aux_todos['dtoperacao']
    fv['data_emissao'] = mtm_aux_todos['data_emissao']
    fv['data_expiracao'] = mtm_aux_todos['data_expiracao']
    fv['data_inicio_rent'] = mtm_aux_todos['dt_inicio_rentab']
    fv['data_primeiro_pagamento_juros'] = mtm_aux_todos['data_primeiro_pagamento_juros']
    fv['valor_nominal'] = mtm_aux_todos['valor_nominal']
    fv['indexador'] = mtm_aux_todos['indexador']
    fv['indexador_dc_du'] = mtm_aux_todos['indexador_dc_du']
    fv['percentual_indexador'] = mtm_aux_todos['percentual_indexador']
    fv['tipo_capitalizacao'] = mtm_aux_todos['tipo_capitalizacao']
    fv['juros_dc_du'] = mtm_aux_todos['juros_dc_du']
    fv['juros_cada'] = mtm_aux_todos['juros_cada']
    fv['juros_unidade'] = mtm_aux_todos['juros_unidade']
    fv['taxa_juros'] = mtm_aux_todos['taxa_juros']
    fv['tipo'] = mtm_aux_todos['tipo']
    fv['data_ref'] = mtm_aux_todos['dt_ref']
    fv['data_ref2'] = mtm_aux_todos['dt_ref2']
    fv['flag_inclusao'] = mtm_aux_todos['flag_inclusao']
    fv['juros_tipo'] = mtm_aux_todos['juros_tipo']
    fv['indexador_tipo'] = mtm_aux_todos['index_tipo']
    # fv['indexador_juros'] = mtm_aux_todos['indexador_juros']
    fv['data_inicio'] = mtm_aux_todos['dt_inicio']
    fv['ano_inicio'] = mtm_aux_todos['ano_inicio']
    fv['mes_inicio'] = mtm_aux_todos['mes_inicio']
    fv['dia_inicio'] = mtm_aux_todos['dia_inicio']
    fv['indice_du_inicio'] = mtm_aux_todos['indice_du_inicio']
    fv['indice_dc_inicio'] = mtm_aux_todos['indice_dc_inicio']
    fv['data_fim'] = mtm_aux_todos['dt_fim']
    fv['ano_fim'] = mtm_aux_todos['ano_fim']
    fv['mes_fim'] = mtm_aux_todos['mes_fim']
    fv['dia_fim'] = mtm_aux_todos['dia_fim']
    fv['indice_du_fim'] = mtm_aux_todos['indice_du_fim']
    fv['indice_dc_fim'] = mtm_aux_todos['indice_dc_fim']
    fv['du_per'] = mtm_aux_todos['du_per']
    fv['dc_per'] = mtm_aux_todos['dc_per']
    fv['tx_acum_du_inicio'] = mtm_aux_todos['tx_acum_du_inicio']
    fv['tx_acum_dc_inicio'] = mtm_aux_todos['tx_acum_dc_inicio']
    fv['tx_acum_du_fim'] = mtm_aux_todos['tx_acum_du_fim']
    fv['tx_acum_dc_fim'] = mtm_aux_todos['tx_acum_dc_fim']
    fv['tx_acum_fator_di_inicio'] = mtm_aux_todos['tx_acum_fator_di_inicio']
    fv['tx_acum_fator_di_fim'] = mtm_aux_todos['tx_acum_fator_di_fim']
    fv['fator_index_per'] = mtm_aux_todos['fator_index_per']
    fv['fator_index_acum'] = mtm_aux_todos['fator_index_acum']
    # fv['fator_index_acum_lag'] = mtm_aux_todos['fator_index_acum_lag']
    fv['perc_amortizacao'] = mtm_aux_todos['perc_amortizacao']
    fv['pagto_amortizacao'] = mtm_aux_todos['pagto_amortizacao']
    fv['perc_amortizacao_acum'] = mtm_aux_todos['perc_amortizacao_acum']
    # fv['perc_amortizacao_acum_lag'] = mtm_aux_todos['perc_amortizacao_acum_lag']
    fv['perc_amortizacao_acum_dt'] = mtm_aux_todos['perc_amortizacao_acum_dt']
    fv['fator_amortizacao_acum_dt'] = mtm_aux_todos['fator_amortizacao_acum_dt']
    # fv['fator_amortizacao_acum_dt_lag'] = mtm_aux_todos['fator_amortizacao_acum_dt_lag']
    fv['vne'] = mtm_aux_todos['vna']
    fv['vna'] = mtm_aux_todos['vne']
    fv['vl_amortizacao'] = mtm_aux_todos['vl_amortizacao']
    fv['principal_perc'] = mtm_aux_todos['principal_perc']
    fv['juros_tipo1'] = mtm_aux_todos['juros_tipo1']
    fv['fator_juros_per_du_exp'] = mtm_aux_todos['fator_juros_per_du_exp']
    fv['fator_juros_per_dc_exp'] = mtm_aux_todos['fator_juros_per_dc_exp']
    fv['fator_juros_per_du_lin'] = mtm_aux_todos['fator_juros_per_du_lin']
    fv['fator_juros_per_dc_lin'] = mtm_aux_todos['fator_juros_per_dc_lin']
    fv['fator_juros_per'] = mtm_aux_todos['fator_juros_per']
    fv['juros_per'] = mtm_aux_todos['juros_per']
    fv['percentual_juros'] = mtm_aux_todos['percentual_juros']
    fv['pagto_juros'] = mtm_aux_todos['pagto_juros']
    fv['saldo_dev_juros'] = mtm_aux_todos['saldo_dev_juros']
    fv['pagto_juros_perc'] = mtm_aux_todos['pagto_juros_perc']
    fv['saldo_dev_juros_perc'] = mtm_aux_todos['saldo_dev_juros_perc']
    fv['valor_nominal_data'] = mtm_aux_todos['valor_nominal_data']
    fv['fv'] = mtm_aux_todos['fv']

    ###############################################################################
    # 12 - Criação das ETTJS
    ###############################################################################


    ###############################################################################
    # 13 - Valor presente
    ###############################################################################
    tp_mtm = fv.copy()

    dtbase_mtm = str(dtbase_mtm)
    ano = str(dtbase_mtm)[0:4]
    mes = str(dtbase_mtm)[5:7]
    dia = str(dtbase_mtm)[8:10]
    data = datetime.date(int(ano), int(mes), int(dia))

    tp_mtm['ano_mtm'] = int(ano)
    tp_mtm['mes_mtm'] = int(mes)
    tp_mtm['dia_mtm'] = int(dia)

    # del inicio['id_bc_series']

    mtm_dia = inicio.copy()
    mtm_dia = mtm_dia.rename(columns={'ano_inicio': 'ano_mtm', 'mes_inicio': 'mes_mtm', 'dia_inicio': 'dia_mtm',
                                      'indice_dc_inicio': 'indice_dc_mtm', 'indice_du_inicio': 'indice_du_mtm'})
    mtm_dia = mtm_dia.sort(['ano_mtm', 'mes_mtm', 'dia_mtm', 'indexador'], ascending=[True, True, True, True])
    mtm_dia = mtm_dia.drop_duplicates(subset=['ano_mtm', 'mes_mtm', 'dia_mtm'], take_last=False)
    del mtm_dia['tx_acum_du_inicio']
    del mtm_dia['tx_acum_dc_inicio']
    del mtm_dia['indexador']

    tp_mtm = tp_mtm.merge(mtm_dia, on=['ano_mtm', 'mes_mtm', 'dia_mtm'], how='left')

    tp_mtm['ano_dt_ref2'] = pd.to_datetime(tp_mtm['data_ref2']).dt.year
    tp_mtm['mes_dt_ref2'] = pd.to_datetime(tp_mtm['data_ref2']).dt.month
    tp_mtm['dia_dt_ref2'] = pd.to_datetime(tp_mtm['data_ref2']).dt.day

    dt_ref_dia = mtm_dia.copy()

    dt_ref_dia = dt_ref_dia.rename(columns={'ano_mtm': 'ano_dt_ref2', 'mes_mtm': 'mes_dt_ref2', 'dia_mtm': 'dia_dt_ref2',
                                            'indice_dc_mtm': 'indice_dc_dt_ref2', 'indice_du_mtm': 'indice_du_dt_ref2'})
    tp_mtm = tp_mtm.merge(dt_ref_dia, on=['ano_dt_ref2', 'mes_dt_ref2', 'dia_dt_ref2'], how='left')

    tp_mtm['prazo_du'] = tp_mtm['indice_du_dt_ref2'] - tp_mtm['indice_du_mtm']
    tp_mtm['prazo_dc'] = tp_mtm['indice_dc_dt_ref2'] - tp_mtm['indice_dc_mtm']

    ###############################################################################
    # Volta indexador dos papéis TJLP para PRE para poder trazer a valor presente
    tp_mtm['indexador'] = np.where(tp_mtm['codigo_isin'].isin(['BRBNDPDBS0B9']), 'PRE', tp_mtm['indexador'])
    ###############################################################################


    if len(tp_mtm[tp_mtm.indexador == 'PRE']) != 0:
        maximo_tp_PRE = max(tp_mtm['prazo_du'][tp_mtm.indexador == 'PRE'])

    if len(tp_mtm[tp_mtm.indexador == 'IGP']) != 0:
        maximo_tp_IGPM = max(tp_mtm['prazo_du'][tp_mtm.indexador == 'IGP'])

    if len(tp_mtm[tp_mtm.indexador == 'IPCA']) != 0:
        maximo_tp_IPCA = max(tp_mtm['prazo_du'][tp_mtm.indexador == 'IPCA'])

    ###############################################################################
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')
    query = 'select * from projeto_inv.curva_ettj_interpol_' + ano + "_" + mes + ' where indexador_cod in("PRE","DIM","DIC");'
    ettj = pd.read_sql(query, con=connection)
    # Seleciona a última carga
    ettj = ettj.sort(['indexador_cod', 'dt_ref', 'data_bd'], ascending=[True, False, False])
    ettj = ettj.drop_duplicates(subset=['prazo', 'tx_spot', 'tx_spot_ano', 'tx_termo_dia', 'indexador_cod'],
                                take_last=False)
    ettj['indexador'] = np.where(ettj['indexador_cod'] == 'DIC', 'IPCA',
                                 np.where(ettj['indexador_cod'] == 'DIM', 'IGP', 'PRE'))
    ettj = ettj.rename(columns={'prazo': 'prazo_du'})
    ettj_filtro = ettj[['prazo_du', 'tx_spot', 'tx_spot_ano', 'indexador']]
    ###############################################################################

    # Extrapolação PRE, se necessário
    if len(tp_mtm[tp_mtm.indexador == 'PRE']) != 0:
        maximo_ettj = max(ettj_filtro['prazo_du'][ettj_filtro.indexador == 'PRE'])
        # del ettj_fluxo
        if maximo_ettj < max(tp_mtm['prazo_du'][tp_mtm.indexador == 'PRE']):
            ettj_filtro_PRE = ettj_filtro[['prazo_du', 'tx_spot_ano', 'indexador']][ettj_filtro.indexador == 'PRE'].copy()
            ettj_filtro_PRE = ettj_filtro_PRE.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
            ettj_filtro_PRE = ettj_filtro_PRE[0:maximo_ettj - 1].copy()

            tx_max = ettj_filtro_PRE['tx_spot_ano'].loc[len(ettj_filtro_PRE) - 1]

            ettj_aux = pd.DataFrame(columns=['prazo_du', 'indexador'])
            ettj_aux['prazo_du'] = np.linspace(1, maximo_tp_PRE, maximo_tp_PRE)
            ettj_aux['indexador'] = 'PRE'
            ettj_aux = ettj_aux.merge(ettj_filtro_PRE, on=['prazo_du', 'indexador'], how='left')
            ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
            ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du'] / 252) - 1
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_aux.copy()
        else:
            ettj_aux = ettj_filtro.copy()
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_aux.copy()
    else:
        ettj_fluxo = ettj_filtro.copy()

    # Extrapolação IGPM, se necessário
    if len(tp_mtm[tp_mtm.indexador == 'IGP']) != 0:
        maximo_ettj = max(ettj_filtro['prazo_du'][ettj_filtro.indexador == 'IGP'])
        # del ettj_fluxo
        if maximo_ettj < max(tp_mtm['prazo_du'][tp_mtm.indexador == 'IGP']):
            ettj_filtro_IGPM = ettj_filtro[['prazo_du', 'tx_spot_ano', 'indexador']][ettj_filtro.indexador == 'IGP'].copy()
            ettj_filtro_IGPM = ettj_filtro_IGPM.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
            ettj_filtro_IGPM = ettj_filtro_IGPM[0:maximo_ettj - 1].copy()

            tx_max = ettj_filtro_IGPM['tx_spot_ano'].loc[len(ettj_filtro_IGPM) - 1]

            ettj_aux = pd.DataFrame(columns=['prazo_du', 'indexador'])
            ettj_aux['prazo_du'] = np.linspace(1, maximo_tp_IGPM, maximo_tp_IGPM)
            ettj_aux['indexador'] = 'IGP'
            ettj_aux = ettj_aux.merge(ettj_filtro_IGPM, on=['prazo_du', 'indexador'], how='left')
            ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
            ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du'] / 252) - 1
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_fluxo.append(ettj_aux)
        else:
            ettj_aux = ettj_filtro.copy()
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_aux.copy()
    else:
        ettj_fluxo = ettj_filtro.copy()

    # Extrapolação IPCA, se necessário
    if len(tp_mtm[tp_mtm.indexador == 'IPCA']) != 0:
        maximo_ettj = max(ettj_filtro['prazo_du'][ettj_filtro.indexador == 'IPCA'])
        # del ettj_fluxo
        if maximo_ettj < max(tp_mtm['prazo_du'][tp_mtm.indexador == 'IPCA']):
            ettj_filtro_IPCA = ettj_filtro[['prazo_du', 'tx_spot_ano', 'indexador']][ettj_filtro.indexador == 'IPCA'].copy()
            ettj_filtro_IPCA = ettj_filtro_IPCA.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
            ettj_filtro_IPCA = ettj_filtro_IPCA[0:maximo_ettj - 1].copy()

            tx_max = ettj_filtro_IPCA['tx_spot_ano'].loc[len(ettj_filtro_IPCA) - 1]

            ettj_aux = pd.DataFrame(columns=['prazo_du', 'indexador'])
            ettj_aux['prazo_du'] = np.linspace(1, maximo_tp_IPCA, maximo_tp_IPCA)
            ettj_aux['indexador'] = 'IGP'
            ettj_aux = ettj_aux.merge(ettj_filtro_IPCA, on=['prazo_du', 'indexador'], how='left')
            ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
            ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du'] / 252) - 1
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_fluxo.append(ettj_aux)
        else:
            ettj_aux = ettj_filtro.copy()
            del ettj_aux['tx_spot_ano']
            ettj_fluxo = ettj_aux.copy()
    else:
        ettj_fluxo = ettj_filtro.copy()

    tp_mtm = tp_mtm.merge(ettj_fluxo, on=['prazo_du', 'indexador'], how='left')

    tp_mtm['tx_spot'] = tp_mtm['tx_spot'].fillna(0)
    tp_mtm['fator_desconto'] = 0
    tp_mtm['fator_desconto'][(tp_mtm.prazo_du > 0)] = 1 / (1 + tp_mtm['tx_spot'][tp_mtm.prazo_du > 0])
    tp_mtm['tx_spot_ano'] = (1 + tp_mtm['tx_spot']) ** (252 / tp_mtm['prazo_du']) - 1
    tp_mtm['tx_spot_ano_DV100'] = tp_mtm['tx_spot_ano'] + 0.01
    tp_mtm['tx_spot_DV100'] = (1 + tp_mtm['tx_spot_ano_DV100']) ** (tp_mtm['prazo_du'] / 252) - 1
    tp_mtm['fator_desconto_DV100'] = 0
    tp_mtm['fator_desconto_DV100'][tp_mtm.prazo_du > 0] = 1 / (1 + tp_mtm['tx_spot_DV100'][tp_mtm.prazo_du > 0])

    tp_mtm['pv'] = tp_mtm['fv'] * tp_mtm['fator_desconto']
    tp_mtm['pv'] = tp_mtm['pv'].fillna(0)
    tp_mtm['pv_DV100'] = tp_mtm['fv'] * tp_mtm['fator_desconto_DV100']

    mtm = tp_mtm[['codigo_isin', 'pv']].copy()
    mtm_pv = mtm.groupby(['codigo_isin']).sum().reset_index()
    mtm_pv = mtm_pv.rename(columns={'pv': 'mtm'})
    tp_mtm = tp_mtm.merge(mtm_pv, on=['codigo_isin'], how='left')

    mtm = tp_mtm[['codigo_isin', 'pv_DV100']].copy()
    mtm_pv = mtm.groupby(['codigo_isin']).sum().reset_index()
    mtm_pv = mtm_pv.rename(columns={'pv_DV100': 'mtm_DV100'})
    tp_mtm = tp_mtm.merge(mtm_pv, on=['codigo_isin'], how='left')

    tp_mtm['mtm_DV100'] = tp_mtm['mtm_DV100'].fillna(0)

    tp_mtm['perc_mtm'] = np.where(tp_mtm['mtm'] != 0, tp_mtm['pv'] / tp_mtm['mtm'], 0)
    tp_mtm['perc_mtm'] = tp_mtm['perc_mtm'].fillna(0)

    ###############################################################################
    # 14 - Duration. DV100
    ###############################################################################

    # Cálculo da Duration
    tp_mtm['vertices_positivo'] = np.where(tp_mtm['prazo_du'] > 0, tp_mtm['prazo_du'], 0)
    tp_mtm['fluxo_positivo'] = np.where(tp_mtm['prazo_du'] > 0, tp_mtm['pv'], 0)
    tp_mtm['fluxo_ponderado'] = tp_mtm['vertices_positivo'] * tp_mtm['fluxo_positivo']
    # Não está funcionando. Está tudo vazio

    x = tp_mtm[['codigo_isin', 'fluxo_positivo', 'fluxo_ponderado']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'fluxo_positivo_sum', 'fluxo_ponderado_sum'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['fluxo_ponderado_sum'] = x['fluxo_ponderado']
    x1['fluxo_positivo_sum'] = x['fluxo_positivo']
    tp_mtm = tp_mtm.merge(x1, on=['codigo_isin'], how='left')
    tp_mtm['duration'] = np.where(tp_mtm['fluxo_positivo_sum'] != 0,
                                  tp_mtm['fluxo_ponderado_sum'] / tp_mtm['fluxo_positivo_sum'], 0)
    tp_mtm['duration'] = tp_mtm['duration'].fillna(0)

    # Calcula o DV100
    tp_mtm['DV100'] = tp_mtm['mtm'] - tp_mtm['mtm_DV100']
    tp_mtm['DV100'] = tp_mtm['DV100'].fillna(0)

    mtm_isin = tp_mtm[['codigo_isin', 'mtm']].copy()
    mtm_isin = mtm_isin.drop_duplicates()

    ###############################################################################
    # 15 - Impressão
    ###############################################################################

    tp_mtm['tx_acum_fator_di_inicio'] = tp_mtm['tx_acum_fator_di_inicio'].fillna(0)
    tp_mtm['tx_acum_fator_di_fim'] = tp_mtm['tx_acum_fator_di_fim'].fillna(0)
    tp_mtm['fator_amortizacao_acum_dt'] = tp_mtm['fator_amortizacao_acum_dt'].fillna(0)
    tp_mtm['perc_amortizacao_acum_dt'] = tp_mtm['perc_amortizacao_acum_dt'].fillna(0)

    del tp_mtm['data_ref2']
    del tp_mtm['fator_index_acum_lag']
    del tp_mtm['perc_amortizacao_acum_lag']
    del tp_mtm['fator_amortizacao_acum_dt_lag']

    # Reinverte id_papel e codigo_isin
    tp_mtm['codigo_isin_temp'] = tp_mtm['id_papel']
    tp_mtm['id_papel'] = tp_mtm['codigo_isin']
    tp_mtm['codigo_isin'] = tp_mtm['codigo_isin_temp']

    # Retira o código_isin do id_papel - SEMPRE FILTRAR TB PELO TIPO DE ATIVO
    tp_mtm['id_papel'] = tp_mtm['id_papel'].str.split('-')
    tp_mtm['id_papel'] = tp_mtm['id_papel'].str[0]

    del tp_mtm['codigo_isin_temp']
    del tp_mtm['tx_spot_ano']
    del tp_mtm['tx_spot_ano_DV100']
    del tp_mtm['tx_spot_DV100']

    tp_mtm.to_excel(save_path_mtm_titprivado)

    tp_mtm['data_bd'] = datetime.datetime.today()

    ##Conexão com Banco de Dados
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')
    pd.io.sql.to_sql(tp_mtm, name='mtm_titprivado', con=connection, if_exists='append', flavor='mysql', index=0)
    connection.close()
