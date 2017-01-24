def mtm_titpublico():

    import datetime
    import pandas as pd
    import numpy as np
    import pymysql as db
    from findt import FinDt
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    # Diretório de save de planilhas
    save_path = full_path_from_database('mtm_titpublico_output') + 'erro.xlsx'
    feriados_sheet = full_path_from_database('feriados_nacionais') + 'feriados_nacionais.csv'

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dt_base = dtbase[0] + '-' + dtbase[1] + '-' + dtbase[2]
    # Data formatada como Dia-Mes-Ano
    data_inicio = dtbase[2] + '-' + dtbase[1] + '-' + dtbase[0]

    # Conexão com Banco de Dados
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    query = 'SELECT a.* FROM projeto_inv.anbima_tpf a right join (select dt_referencia, max(dt_carga) as dt_carga from projeto_inv.anbima_tpf where dt_referencia="' + dt_base + '" group by 1) b on a.dt_referencia=b.dt_referencia and a.dt_carga=b.dt_carga;'
    basetaxa = pd.read_sql(query, con=connection)
    basetaxa = basetaxa.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    basetaxa1 = basetaxa[["titulo", "cod_selic", "dt_emissao", "dt_vencto", "dt_referencia", "tx_indicativa", "pu"]]

    query = 'SELECT a.* FROM projeto_inv.anbima_fluxo_tpf a right join (select titulo, cod_selic, dt_emissao, dt_vencto, max(data_bd) as data_bd from projeto_inv.anbima_fluxo_tpf group by 1,2,3,4) b on a.titulo=b.titulo and a.cod_selic=b.cod_selic and a.dt_emissao=b.dt_emissao and a.dt_vencto=b.dt_vencto and a.data_bd=b.data_bd;'
    baseref1 = pd.read_sql(query, con=connection)

    baseref1 = pd.merge(baseref1, basetaxa1, left_on=['titulo', 'cod_selic', 'dt_emissao', 'dt_vencto'],
                        right_on=['titulo', 'cod_selic', 'dt_emissao', 'dt_vencto'], how='right')

    baseref2 = baseref1.reindex(
        columns=['titulo', 'cod_selic', 'dt_emissao', 'dt_vencto', 'dt_vencto2', 'tx_indicativa', 'dt_referencia', 'cupom',
                 'dt_ref', 'fv'])

    anbima_fluxomtm_tpf = baseref2[baseref2['dt_ref'] >= baseref2['dt_referencia']].copy()

    dt_max = max(anbima_fluxomtm_tpf['dt_ref'])
    dt_max = dt_max.strftime('%d-%m-%Y')
    per = FinDt.DatasFinanceiras(data_inicio, dt_max, path_arquivo = feriados_sheet)
    du = pd.DataFrame(columns=['dt_ref'])
    dc = pd.DataFrame(columns=['dt_ref'])
    dc['dt_ref'] = per.dias()
    du['dt_ref'] = per.dias(3)
    du['flag_du'] = 1
    controle_du = pd.merge(dc, du, left_on=['dt_ref'], right_on=['dt_ref'], how='left')
    controle_du = controle_du.fillna(0)
    controle_du['du'] = np.cumsum(controle_du['flag_du']).astype(float)

    anbima_fluxomtm_tpf = pd.merge(anbima_fluxomtm_tpf, controle_du, left_on=['dt_ref'], right_on=['dt_ref'], how='left')

    dt_base1 = dt_base + ' 00:00:00'
    ###############################################################################
    '20161228'
    # CHAMBER DE ULTIMO CASO SE POR ACASO NAO TIVER O VNA DA DATA BASE. PEGAR O DIA MAIS PROXIMO QUE TIVER NA BASE
    # UUUUUUULTIMO CASO
    # dt_base1 = "2016-11-29 00:00:00"
    ###############################################################################
    query = 'select a.codigo_selic, a.titulo, a.vna from projeto_inv.anbima_vna a right join (select titulo, codigo_selic, data_referencia, max(data_bd) as data_bd from projeto_inv.anbima_vna where data_referencia="' + dt_base1 + '" group by 1,2,3) b on a.titulo=b.titulo and a.codigo_selic=b.codigo_selic and a.data_referencia=b.data_referencia and a.data_bd=b.data_bd;'
    vna = pd.read_sql(query, con=connection)

    anbima_fluxomtm_tpf['titulo'] = np.where(anbima_fluxomtm_tpf['titulo'] == 'LFT', 'lft', anbima_fluxomtm_tpf['titulo'])
    anbima_fluxomtm_tpf['titulo'] = np.where(anbima_fluxomtm_tpf['titulo'] == 'LTN', 'ltn', anbima_fluxomtm_tpf['titulo'])
    anbima_fluxomtm_tpf['titulo'] = np.where(anbima_fluxomtm_tpf['titulo'] == 'NTN-F', 'ntnf',
                                             anbima_fluxomtm_tpf['titulo'])
    anbima_fluxomtm_tpf['titulo'] = np.where(anbima_fluxomtm_tpf['titulo'] == 'NTN-C', 'ntnc',
                                             anbima_fluxomtm_tpf['titulo'])
    anbima_fluxomtm_tpf['titulo'] = np.where(anbima_fluxomtm_tpf['titulo'] == 'NTN-B', 'ntnb',
                                             anbima_fluxomtm_tpf['titulo'])

    anbima_fluxomtm_tpf['cotacao'] = 0
    anbima_fluxomtm_tpf['fv1'] = 0
    anbima_fluxomtm_tpf['pv'] = 0

    anbima_fluxomtm_tpf = pd.merge(anbima_fluxomtm_tpf, vna, left_on=['cod_selic', 'titulo'],
                                   right_on=['codigo_selic', 'titulo'], how='left')
    del anbima_fluxomtm_tpf['codigo_selic']
    anbima_fluxomtm_tpf.ix[anbima_fluxomtm_tpf.titulo == 'ltn', 'vna'] = 1000
    anbima_fluxomtm_tpf.ix[anbima_fluxomtm_tpf.titulo == 'ntnf', 'vna'] = 1000

    anbima_fluxomtm_tpf1 = anbima_fluxomtm_tpf.copy()
    anbima_fluxomtm_tpf = anbima_fluxomtm_tpf1.copy()

    anbima_fluxomtm_tpf['dif'] = (pd.to_datetime(anbima_fluxomtm_tpf['dt_vencto2']) - pd.to_datetime(
        anbima_fluxomtm_tpf['dt_ref'])).astype('timedelta64[D]') * 1
    anbima_fluxomtm_tpf['fv'] = np.where(anbima_fluxomtm_tpf.dif <= 10.0, (1 + (anbima_fluxomtm_tpf['cupom'])) ** 0.5,
                                         (1 + (anbima_fluxomtm_tpf['cupom'])) ** 0.5 - 1)
    anbima_fluxomtm_tpf.ix[anbima_fluxomtm_tpf.titulo == 'ltn', 'fv'] = 1
    anbima_fluxomtm_tpf.ix[anbima_fluxomtm_tpf.titulo == 'lft', 'fv'] = 1
    del anbima_fluxomtm_tpf['dif']

    anbima_fluxomtm_tpf['du_ajustado'] = np.where(anbima_fluxomtm_tpf['dt_vencto2'] == anbima_fluxomtm_tpf['dt_ref'],
                                                  anbima_fluxomtm_tpf['du'],
                                                  anbima_fluxomtm_tpf['du'] + anbima_fluxomtm_tpf['flag_du'])

    titpublico = pd.DataFrame()

    ltn = anbima_fluxomtm_tpf[anbima_fluxomtm_tpf['titulo'] == 'ltn'].copy()
    formcor = np.float64((ltn['vna'] * ltn['fv'] / ((1 + ltn['tx_indicativa'] / 100) ** (ltn['du'] / 252))))
    ltn['pv'] = np.trunc(formcor * (10 ** 6)) / (10 ** 6)
    titpublico = titpublico.append(ltn)

    lft = anbima_fluxomtm_tpf[anbima_fluxomtm_tpf['titulo'] == 'lft'].copy()
    lft['cotacao'] = np.trunc(1 / (1 + lft['tx_indicativa'] / 100) ** (lft['du'] / 252) * (10 ** 6)) / (10 ** 6)
    formcor = np.float64(lft['vna'] * lft['cotacao'] * (10 ** 6))
    lft['pv'] = np.trunc(formcor) / (10 ** 6)
    titpublico = titpublico.append(lft)

    ntnf = anbima_fluxomtm_tpf[anbima_fluxomtm_tpf['titulo'] == 'ntnf'].copy()
    formcor = np.float64(ntnf['fv'] * ntnf['vna'] * (10 ** 9))
    ntnf['fv1'] = np.trunc(formcor) / (10 ** 9)
    ntnf['pv'] = np.trunc((ntnf['fv1'] / (1 + ntnf['tx_indicativa'] / 100) ** (ntnf['du'] / 252)) * (10 ** 7)) / (10 ** 7)
    titpublico = titpublico.append(ntnf)

    ntnc = anbima_fluxomtm_tpf[anbima_fluxomtm_tpf['titulo'] == 'ntnc'].copy()
    formcor = np.float64(
        (np.trunc((ntnc['fv'] / (1 + ntnc['tx_indicativa'] / 100) ** (ntnc['du'] / 252)) * (10 ** 8)) / (10 ** 8)) * ntnc[
            'vna'] * (10 ** 6))
    ntnc['pv'] = np.trunc(formcor) / (10 ** 6)
    titpublico = titpublico.append(ntnc)

    ntnb = anbima_fluxomtm_tpf[anbima_fluxomtm_tpf['titulo'] == 'ntnb'].copy()
    formcor = np.float64(
        (np.trunc((ntnb['fv'] / (1 + ntnb['tx_indicativa'] / 100) ** (ntnb['du'] / 252)) * (10 ** 8)) / (10 ** 8)) * ntnb[
            'vna'] * (10 ** 6))
    ntnb['pv'] = np.trunc(formcor) / (10 ** 6)
    titpublico = titpublico.append(ntnb)

    del titpublico['cotacao']
    del titpublico['fv1']

    mtm_sum_tpf = titpublico.groupby(["titulo", "dt_vencto"], as_index=False).sum()
    mtm_sum_tpf.rename(columns={'pv': 'pu_calc'}, inplace=True)
    anbima_mtm_sum_tpf = mtm_sum_tpf[["titulo", "dt_vencto", "pu_calc"]]
    anbima_fluxomtm_tpf = pd.merge(titpublico, anbima_mtm_sum_tpf, left_on=["titulo", "dt_vencto"],
                                   right_on=["titulo", "dt_vencto"], how='left')

    anbima_fluxomtm_tpf['perc_pu_fluxo'] = anbima_fluxomtm_tpf['pv'] / anbima_fluxomtm_tpf['pu_calc']

    anbima_fluxomtm_tpf['data_bd'] = datetime.datetime.today()

    # Salvar no MySQL
    pd.io.sql.to_sql(anbima_fluxomtm_tpf, name='anbima_fluxomtm_tpf', con=connection, if_exists='append', flavor='mysql',
                     index=0)

    # validacao
    basetaxa1['titulo'] = np.where(basetaxa1['titulo'] == 'LFT', 'lft', basetaxa1['titulo'])
    basetaxa1['titulo'] = np.where(basetaxa1['titulo'] == 'LTN', 'ltn', basetaxa1['titulo'])
    basetaxa1['titulo'] = np.where(basetaxa1['titulo'] == 'NTN-F', 'ntnf', basetaxa1['titulo'])
    basetaxa1['titulo'] = np.where(basetaxa1['titulo'] == 'NTN-C', 'ntnc', basetaxa1['titulo'])
    basetaxa1['titulo'] = np.where(basetaxa1['titulo'] == 'NTN-B', 'ntnb', basetaxa1['titulo'])

    anbima_mtm_sum_tpf = anbima_mtm_sum_tpf.merge(basetaxa1, on=['titulo', 'dt_vencto'], how='left')
    anbima_mtm_sum_tpf['erro'] = anbima_mtm_sum_tpf['pu'] - anbima_mtm_sum_tpf['pu_calc']
    anbima_mtm_sum_tpf.to_excel(save_path, sheet_name='titpublico')
    # obs.: não bate 100% pq o truncamento deveria ser feito na soma e não por pagamento. No entanto, como aqui só queremos saber o % do MtM correspondente ao pagamento, fizemos uma aproximação
