def mtm_curva_titprivado():

    import datetime, time
    import pandas as pd
    import numpy as np
    import pymysql as db
    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    # Diretório de save de planilhas
    save_path_puposicao = full_path_from_database('get_output_quadro419') + 'puposicao_final.xlsx'
    save_path_titprivado_perc = full_path_from_database('get_output_quadro419') + 'titprivado_perc.xlsx'

    tol = 0.20

    writer = ExcelWriter(save_path_puposicao)

    horario_bd = datetime.datetime.today()

    ###############################################################################
    # 1 - Leitura e criação de tabelas
    ###############################################################################

    # Informações do cálculo de MTM
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', charset='utf8')

    query = 'select * from projeto_inv.mtm_titprivado'
    #query = "SELECT * tipo_ativo FROM mtm_titprivado WHERE tipo_ativo <> 'DBS' AND tipo_ativo <> 'CTF'"

    mtm_titprivado0 = pd.read_sql(query, con=connection)

    mtm_titprivado = mtm_titprivado0.copy()

    # Tira debentures
    mtm_titprivado = mtm_titprivado[(mtm_titprivado.tipo_ativo != 'DBS') & (mtm_titprivado.tipo_ativo != 'CTF')]

    mtm_titprivado['dtrel'] = mtm_titprivado['id_papel'].str.split('_')
    mtm_titprivado['dtrel'] = mtm_titprivado['dtrel'].str[0]

    mtm_titprivado = mtm_titprivado[mtm_titprivado.dtrel == dtbase_concat].copy()
    mtm_titprivado = mtm_titprivado[mtm_titprivado.data_bd == max(mtm_titprivado.data_bd)]

    del mtm_titprivado['dtrel']

    mtm_titprivado = mtm_titprivado.rename(columns={'data_fim': 'dt_ref', 'dtoperacao': 'dtoperacao_mtm'})

    mtm_titprivado['dt_ref'] = pd.to_datetime(mtm_titprivado['dt_ref'])
    mtm_titprivado['dt_ref'] = np.where(mtm_titprivado['indexador'] == 'DI1',
                                        mtm_titprivado['dt_ref'] + pd.DateOffset(months=0, days=1),
                                        mtm_titprivado['dt_ref'])
    mtm_titprivado['dt_ref'] = mtm_titprivado['dt_ref'].dt.date

    # Altera o nome do id_papel para levar em consideração o flag
    mtm_titprivado['id_papel_old'] = mtm_titprivado['id_papel']
    mtm_titprivado['id_papel'] = mtm_titprivado['id_papel_old'].str.split('_')
    mtm_titprivado['id_papel'] = mtm_titprivado['id_papel'].str[0] + '_' + mtm_titprivado['id_papel'].str[1] + '_' + \
                                 mtm_titprivado['id_papel'].str[2]

    # Retuira papéis com valor nominal zerado ou vazio
    mtm_titprivado = mtm_titprivado[(mtm_titprivado.valor_nominal.notnull()) & (mtm_titprivado.valor_nominal != 0)].copy()

    del mtm_titprivado['data_bd']

    # Informações XML

    query = 'SELECT * FROM projeto_inv.xml_titprivado_org'

    xml_titprivado = pd.read_sql(query, con=connection)

    connection.close()

    xml_titprivado['dtrel'] = xml_titprivado['id_papel'].str.split('_')
    xml_titprivado['dtrel'] = xml_titprivado['dtrel'].str[0]

    xml_titprivado = xml_titprivado[xml_titprivado.dtrel == dtbase_concat].copy()
    xml_titprivado = xml_titprivado[xml_titprivado.data_bd == max(xml_titprivado.data_bd)]

    del xml_titprivado['data_bd']
    del xml_titprivado['dtrel']

    original = xml_titprivado.copy()

    del xml_titprivado['indexador']

    # mtm_titprivado.to_excel(save_path+'checa_id.xlsx')

    titprivado1 = mtm_titprivado.copy()
    # Criação da tabela mtm + xml
    titprivado = xml_titprivado.merge(mtm_titprivado, on=['id_papel'], how='left')

    # Criação da coluna de data de referencia da posição
    titprivado['data_referencia'] = titprivado['id_papel'].str[0:8]
    titprivado['data_referencia'] = pd.to_datetime(titprivado['data_referencia']).dt.date

    ###############################################################################
    # 2 - Escolha do melhor mtm
    ###############################################################################

    titprivado_mercado = titprivado[(titprivado.caracteristica == 'N') & (titprivado.mtm.notnull()) & (titprivado.mtm != 0)]

    titprivado_mercado['dif_mtm'] = titprivado_mercado['puposicao'] / titprivado_mercado['mtm'] - 1
    titprivado_mercado['dif_mtm'] = titprivado_mercado['dif_mtm'].abs()

    titprivado_mercado = titprivado_mercado[titprivado_mercado.dt_ref == titprivado_mercado.data_referencia].copy()

    titprivado_mercado = titprivado_mercado[['id_papel_old', 'id_papel', 'codigo_isin', 'dif_mtm']].copy()

    titprivado_mercado = titprivado_mercado.sort(['dif_mtm'], ascending=[True])
    titprivado_mercado = titprivado_mercado.drop_duplicates(subset=['id_papel'], take_last=False)

    titprivado = titprivado.merge(titprivado_mercado, on=['id_papel_old', 'id_papel', 'codigo_isin'], how='left')

    titprivado = titprivado[
        ((titprivado.caracteristica == 'N') & (titprivado.dif_mtm.notnull())) | (titprivado.caracteristica == 'V')].copy()

    tp_aux = titprivado[['id_papel', 'isin', 'dtvencimento']][titprivado.dif_mtm.isnull()].copy()
    tp_aux = tp_aux.drop_duplicates(subset=['id_papel'])
    tp_aux.to_excel(writer, 'sem_mtm')

    titprivado = titprivado.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################
    # 3 - Cálculo marcação na curva
    ###############################################################################

    titprivado_curva = titprivado[titprivado.caracteristica == 'V'].copy()

    del titprivado_curva['vne']

    # Seleciona a parte do fluxo entre a data da compra e a data da posição
    titprivado_curva = titprivado_curva[titprivado_curva.dt_ref >= titprivado_curva.dtoperacao_mtm].copy()
    titprivado_curva = titprivado_curva[titprivado_curva.dt_ref <= titprivado_curva.data_referencia].copy()

    # Preenchimento do VNE na data da compra
    tp_curva_dtop = titprivado_curva[['id_papel_old',
                                      'saldo_dev_juros_perc',
                                      'pucompra'
                                      ]][titprivado_curva.dt_ref == titprivado_curva.dtoperacao_mtm].copy()

    tp_curva_dtop['vne'] = tp_curva_dtop['pucompra'] * (1 - tp_curva_dtop['saldo_dev_juros_perc'])

    del tp_curva_dtop['saldo_dev_juros_perc']
    del tp_curva_dtop['pucompra']

    titprivado_curva = titprivado_curva.merge(tp_curva_dtop, on=['id_papel_old'], how='left')

    titprivado_curva['principal_perc_acum'] = 1 - titprivado_curva['principal_perc']
    titprivado_curva['principal_perc_acum'] = titprivado_curva[['id_papel_old', 'principal_perc_acum']].groupby(
        ['id_papel_old']).agg(['cumprod'])

    titprivado_curva['vne'] = titprivado_curva['vne'] * titprivado_curva['principal_perc_acum']
    titprivado_curva['pagto_juros'] = titprivado_curva['vne'] * titprivado_curva['pagto_juros_perc']
    titprivado_curva['vna'] = titprivado_curva['vne'] * titprivado_curva['fator_index_per']
    titprivado_curva['vna'][titprivado_curva.indexador == 'DI1'] = titprivado_curva['vne'][
        titprivado_curva.indexador == 'DI1']
    titprivado_curva['saldo_dev_juros'] = titprivado_curva['vna'] * titprivado_curva['saldo_dev_juros_perc']
    titprivado_curva['pupar'] = titprivado_curva['vna'] + titprivado_curva['saldo_dev_juros'] + titprivado_curva[
        'pagto_juros']

    titprivado_curva['dif_curva'] = titprivado_curva['pupar'] / titprivado_curva['puposicao'] - 1
    titprivado_curva['dif_curva'] = titprivado_curva['dif_curva'].abs()

    titprivado_curva = titprivado_curva[titprivado_curva.dt_ref == titprivado_curva.data_referencia].copy()

    titprivado_curva = titprivado_curva[['id_papel_old', 'id_papel', 'codigo_isin', 'dif_curva', 'pupar']].copy()

    titprivado_curva = titprivado_curva.sort(['dif_curva'], ascending=[True])
    titprivado_curva = titprivado_curva.drop_duplicates(subset=['id_papel'], take_last=False)

    titprivado = titprivado.merge(titprivado_curva, on=['id_papel_old', 'id_papel', 'codigo_isin'], how='left')

    titprivado = titprivado[
        ((titprivado.caracteristica == 'V') & (titprivado.dif_curva.notnull())) | (titprivado.caracteristica == 'N')].copy()

    titprivado = titprivado.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    ###############################################################################
    # 4 - Separação dos pu's que ficaram muito distantes do informado
    ###############################################################################

    titprivado['dif_curva'] = titprivado['dif_curva'].fillna(0)
    titprivado['dif_mtm'] = titprivado['dif_mtm'].fillna(0)

    titprivado['dif'] = titprivado['dif_curva'] + titprivado['dif_mtm']

    # Retirar atualizar valores de papéis cujo dif é grande em apenas uma das observações
    x = titprivado[['codigo_isin', 'id_papel']][titprivado.dt_ref == titprivado.data_referencia].groupby(
        ['codigo_isin']).count()
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'count_all'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['count_all'] = x['id_papel']
    titprivado = titprivado.merge(x1, on=['codigo_isin'], how='left')

    x = titprivado[['codigo_isin', 'id_papel']][
        (titprivado.dt_ref == titprivado.data_referencia) & (titprivado.dif > tol) & (
        titprivado.caracteristica == 'N')].groupby(['codigo_isin']).count()
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'count_0'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['count_0'] = x['id_papel']
    titprivado = titprivado.merge(x1, on=['codigo_isin'], how='left')

    titprivado['count_0'] = np.where(titprivado['dif'] < tol, titprivado['count_all'], titprivado['count_0'])
    titprivado['count_dif'] = titprivado['count_all'] - titprivado['count_0']

    x = pd.DataFrame()
    x = titprivado[['id_papel']][(titprivado.count_dif != 0) & (titprivado.dt_ref == titprivado.data_referencia) & (
    titprivado.caracteristica == 'N')].copy()
    x['change'] = 1

    titprivado = titprivado.merge(x, on=['id_papel'], how='left')

    x = pd.DataFrame()
    x = titprivado[['codigo_isin']][(titprivado.count_dif != 0) & (titprivado.dt_ref == titprivado.data_referencia) & (
    titprivado.caracteristica == 'N')].copy()
    x['change_basis'] = 2

    x = x.drop_duplicates()

    titprivado = titprivado.merge(x, on=['codigo_isin'], how='left')

    x = titprivado[['codigo_isin', 'puposicao']][(titprivado.change_basis == 2) & (titprivado.change != 1)].groupby(
        ['codigo_isin']).agg(['mean'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'puposnew'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['puposnew'] = x['puposicao']

    titprivado = titprivado.merge(x1, on=['codigo_isin'], how='left')
    titprivado['puposicao1'] = np.where(titprivado['change'] == 1, titprivado['puposnew'], titprivado['puposicao'])
    titprivado['change'] = titprivado['change'].fillna(0)

    del titprivado['change_basis']

    titprivado['dif'] = titprivado['mtm'] / titprivado['puposicao1'] - 1
    titprivado['dif'] = titprivado['dif'].abs()

    ###############################################################################
    # 4 - Cálculo spread de crédito
    ###############################################################################

    titprivado_spread = \
    titprivado[['id_papel', 'codigo_isin', 'data_referencia', 'puposicao1', 'dt_ref', 'prazo_du', 'pv']][
        (titprivado.caracteristica == 'N')]

    # Tira a média dos puposicao para calcular spread único por isin
    x = titprivado_spread[['codigo_isin', 'puposicao1']].groupby(['codigo_isin']).agg(['mean'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'pumedio'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['pumedio'] = x['puposicao1']
    titprivado_spread = titprivado_spread.merge(x1, on=['codigo_isin'], how='left')

    titprivado_spread['puposicao1'] = titprivado_spread['pumedio']

    del titprivado_spread['pumedio']

    # Seleciona apenas o fluxo com prazo_du positivo
    titprivado_spread = titprivado_spread[titprivado_spread.dt_ref >= titprivado_spread.data_referencia]

    titprivado_spread['pv_pv_fluxo'] = np.where(titprivado_spread['dt_ref'] == titprivado_spread['data_referencia'],
                                                -titprivado_spread['puposicao1'], titprivado_spread['pv'])

    tp_spread = titprivado_spread[['id_papel', 'codigo_isin', 'dt_ref', 'prazo_du', 'pv_pv_fluxo']].copy()
    tp_spread['prazo_du'] = tp_spread['prazo_du'].astype(float)

    id_papel = titprivado_spread['id_papel'].unique()
    spread = np.zeros((len(id_papel)))

    spread_aux = pd.DataFrame(columns=['id_papel', 'spread'])
    spread_aux['id_papel'] = id_papel

    start_time = time.time()
    for i in range(0, len(id_papel)):
        v = tp_spread['pv_pv_fluxo'][tp_spread.id_papel == id_papel[i]].values
        v = np.meshgrid(v, sparse=True)

        s = np.linspace(-0.9999, 0.9999, 100000)
        t = tp_spread['prazo_du'][tp_spread.id_papel == id_papel[i]].values
        t, s = np.meshgrid(t, s, sparse=True)
        f_ts = 1. / (1 + s) ** (t / 252)

        f_spread = v * f_ts

        f_sum = f_spread.sum(axis=1, dtype='float')

        min_index = abs(f_sum).argmin()

        spread[i] = s[min_index]

        #print(time.time() - start_time, i, id_papel[i], spread[i])

        spread_aux['spread'].iloc[i] = spread[i]

    titprivado = titprivado.merge(spread_aux, on=['id_papel'], how='left')

    ###############################################################################
    # 5 - Seleção dos papéis cuja marcação não ficou boa
    ###############################################################################

    tp_bigdif = titprivado[['data_referencia',
                            'codigo_isin',
                            'id_papel',
                            'flag',
                            'caracteristica',
                            'indexador',
                            'dtemissao',
                            'data_emissao',
                            'dtvencimento',
                            'data_expiracao',
                            'valor_nominal',
                            'puemissao',
                            'juros_cada',
                            'coupom',
                            'taxa_juros',
                            'percentual_indexador',
                            'percindex',
                            'perc_amortizacao',
                            'dt_ref',
                            'vne',
                            'du_per',
                            'prazo_du',
                            'fator_index_per',
                            'fator_juros_per',
                            'juros_per',
                            'saldo_dev_juros',
                            'pv',
                            'fv',
                            'mtm',
                            'puposicao',
                            'puposicao1',
                            'change',
                            'count_0',
                            'count_all',
                            'dif',
                            'spread']].copy()

    tp_bigdif['dif'] = tp_bigdif['mtm'] / tp_bigdif['puposicao1'] - 1

    tp_bigdif[
        (tp_bigdif.dif > tol) | (tp_bigdif.dif < -tol) | (tp_bigdif.spread > tol) | (tp_bigdif.spread < -tol)].to_excel(
        writer, 'bigdif')

    ###############################################################################
    # 6 - Atualização do fluxo de percentual de mtm com o spread e carregamento da tabela para preenchimento do quadro 419
    ###############################################################################

    titprivado_perc = titprivado.copy()

    titprivado_perc = titprivado_perc.rename(
        columns={'mtm': 'mtm_old', 'pv': 'pv_old', 'pv_DV100': 'pv_DV100_old', 'fator_desconto': 'fator_desconto_old',
                 'fator_desconto_DV100': 'fator_desconto_DV100_old', 'DV100': 'DV100_old'})

    # Escolhe o melhor spread - SIGNIFICA O MELHOR FLUXO
    titprivado_perc['spread'] = titprivado_perc['spread'].fillna(0)

    # Pega penas uma linha para não ter problemas
    x = titprivado_perc[['id_papel', 'codigo_isin', 'spread']][
        titprivado_perc.dt_ref == titprivado_perc.data_referencia].copy()

    x = x.sort(['codigo_isin', 'spread'], ascending=[True, True])
    x = x.drop_duplicates(subset=['codigo_isin'], take_last=False)

    x['marker'] = 1

    titprivado_perc = titprivado_perc.merge(x, on=['codigo_isin', 'id_papel', 'spread'], how='left')

    titprivado_perc = titprivado_perc[titprivado_perc.marker == 1].copy()
    del titprivado_perc['marker']

    titprivado_perc = titprivado_perc[titprivado_perc.prazo_du >= 0]

    # Recalcula apenas para que está marcado a mercado
    aux = titprivado_perc[titprivado_perc.caracteristica == 'V'].copy()
    aux['mtm_DV100_N'] = 0.0
    aux = aux.rename(columns={'mtm_old': 'mtm'})

    titprivado_perc = titprivado_perc[titprivado_perc.caracteristica == 'N'].copy()

    # Cálculo do fator de desconto atualizado pelo spread
    titprivado_perc['fator_desconto'] = titprivado_perc['fator_desconto_old'] / (1 + titprivado_perc['spread']) ** (
    titprivado_perc['prazo_du'] / 252)
    titprivado_perc['fator_desconto_DV100'] = titprivado_perc['fator_desconto_DV100_old'] / (1 + titprivado_perc[
        'spread']) ** (titprivado_perc['prazo_du'] / 252)

    # Calculo do pv
    titprivado_perc['pv'] = titprivado_perc['fv'] * titprivado_perc['fator_desconto']
    titprivado_perc['pv_DV100'] = titprivado_perc['fv'] * titprivado_perc['fator_desconto_DV100']

    # Calculo do MTM
    x = titprivado_perc[['codigo_isin', 'pv', 'pv_DV100']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'mtm', 'mtm_DV100_N'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['mtm'] = x['pv']
    x1['mtm_DV100_N'] = x['pv_DV100']
    titprivado_perc = titprivado_perc.merge(x1, on=['codigo_isin'], how='left')

    titprivado_perc['mtm_DV100'] = 0.0

    # Escolhe o melhor fluxo
    titprivado_perc['dif_new'] = titprivado_perc['mtm'] - titprivado_perc['puposicao1']
    titprivado_perc['dif_new'] = titprivado_perc['dif_new'].abs()
    titprivado_perc['dif_old'] = titprivado_perc['mtm_old'] - titprivado_perc['puposicao1']
    titprivado_perc['dif_old'] = titprivado_perc['dif_old'].abs()

    titprivado_perc['mtm'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'], titprivado_perc['mtm_old'],
                                      titprivado_perc['mtm'])
    titprivado_perc['mtm_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                            titprivado_perc['mtm_DV100'], titprivado_perc['mtm_DV100_N'])
    titprivado_perc['pv'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'], titprivado_perc['pv_old'],
                                     titprivado_perc['pv'])
    titprivado_perc['pv_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                           titprivado_perc['pv_DV100_old'], titprivado_perc['pv_DV100'])
    titprivado_perc['fator_desconto'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                                 titprivado_perc['fator_desconto_old'], titprivado_perc['fator_desconto'])
    titprivado_perc['fator_desconto_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                                       titprivado_perc['fator_desconto_DV100_old'],
                                                       titprivado_perc['fator_desconto_DV100'])

    # Cálculo do DV100
    titprivado_perc = titprivado_perc.append(aux)

    titprivado_perc['DV100'] = titprivado_perc['mtm'] - titprivado_perc['mtm_DV100']

    # Cálculo do perc_mtm
    titprivado_perc['perc_mtm'] = titprivado_perc['pv'] / titprivado_perc['mtm']
    titprivado_perc['perc_mtm'] = np.where(titprivado_perc['caracteristica'] == 'V',
                                           titprivado_perc['pv_old'] / titprivado_perc['mtm'], titprivado_perc['perc_mtm'])

    # Cálculo da duration
    titprivado_perc['duration'] = titprivado_perc['perc_mtm'] * titprivado_perc['prazo_du']
    x = titprivado_perc[['codigo_isin', 'duration']].groupby(['codigo_isin']).agg(['sum'])
    x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    x1 = pd.DataFrame(columns=['codigo_isin', 'duration'])
    x1['codigo_isin'] = x['codigo_isin']
    x1['duration'] = x['duration']
    del titprivado_perc['duration']
    titprivado_perc = titprivado_perc.merge(x1, on=['codigo_isin'], how='left')

    titprivado_perc[
        ['codigo_isin', 'puposicao1', 'mtm', 'dt_ref', 'mtm_old', 'dif_new', 'dif_old', 'duration', 'spread']].to_excel(
        writer, 'conclusao_fluxo')
    titprivado_perc[titprivado_perc.dt_ref == titprivado_perc.data_referencia][
        ['codigo_isin', 'puposicao1', 'mtm', 'dt_ref', 'mtm_old', 'dif_new', 'dif_old', 'duration', 'spread']].to_excel(
        writer, 'conclusao_resumido')

    finalizacao = titprivado_perc[['codigo_isin', 'mtm']][titprivado_perc.dt_ref == titprivado_perc.data_referencia].copy()

    del titprivado_perc['fator_desconto_DV100_old']
    del titprivado_perc['fator_desconto_old']
    del titprivado_perc['mtm_old']
    del titprivado_perc['mtm_DV100_N']
    del titprivado_perc['pv_DV100_old']
    del titprivado_perc['pv_old']
    del titprivado_perc['DV100_old']
    del titprivado_perc['dif_new']
    del titprivado_perc['dif_old']

    writer.save()

    '''

        Alteração de formato das colunas que são int e foram lidas como float (sabe lá pq...)

    '''

    # id_mtm_titprivado
    titprivado_perc['id_mtm_titprivado'] = titprivado_perc['id_mtm_titprivado'].astype(int)

    # id_bmf_numeraca
    aux = titprivado_perc[['id_papel', 'id_bmf_numeraca']][titprivado_perc.id_bmf_numeraca.notnull()].copy()
    aux['id_bmf_numeraca'] = aux['id_bmf_numeraca'].astype(int)
    del titprivado_perc['id_bmf_numeraca']
    aux = aux.drop_duplicates()
    titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'], how='left')

    # pagto_amortizacao
    aux = titprivado_perc[['id_papel', 'indexador_dc_du']][titprivado_perc.indexador_dc_du.notnull()].copy()
    aux['indexador_dc_du'] = aux['indexador_dc_du'].astype(int)
    del titprivado_perc['indexador_dc_du']
    aux = aux.drop_duplicates()
    titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

    # juros_cada
    aux = titprivado_perc[['id_papel', 'juros_cada']][titprivado_perc.juros_cada.notnull()].copy()
    aux['juros_cada'] = aux['juros_cada'].astype(int)
    del titprivado_perc['juros_cada']
    aux = aux.drop_duplicates()
    titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'], how='left')

    # indexador_dc_du
    aux = titprivado_perc[['id_papel', 'indexador_dc_du']][titprivado_perc.indexador_dc_du.notnull()].copy()
    aux['indexador_dc_du'] = aux['indexador_dc_du'].astype(int)
    del titprivado_perc['indexador_dc_du']
    aux = aux.drop_duplicates()
    titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

    # juros_dc_du
    aux = titprivado_perc[['id_papel', 'juros_dc_du']][titprivado_perc.juros_dc_du.notnull()].copy()
    aux['juros_dc_du'] = aux['juros_dc_du'].astype(int)
    del titprivado_perc['juros_dc_du']
    aux = aux.drop_duplicates()
    titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

    # flag_inclusao
    titprivado_perc['flag_inclusao'] = titprivado_perc['flag_inclusao'].astype(int)

    # du_per
    titprivado_perc['du_per'] = titprivado_perc['du_per'].astype(int)

    # dc_per
    titprivado_perc['dc_per'] = titprivado_perc['dc_per'].astype(int)

    # dt_ref -> data_fim
    titprivado_perc['dt_ref'] = pd.to_datetime(titprivado_perc['dt_ref'])
    titprivado_perc['data_fim'] = np.where(titprivado_perc['indexador'] == 'DI1',
                                           titprivado_perc['dt_ref'] - pd.DateOffset(months=0, days=1),
                                           titprivado_perc['dt_ref'])

    titprivado_perc['id_papel'] = titprivado_perc['id_papel_old']

    titprivado_perc['data_mtm'] = titprivado_perc['data_referencia']
    titprivado_perc['data_negociacao'] = titprivado_perc['data_referencia']

    # Tabelas não necessárias - MTM
    del titprivado_perc['data_referencia']
    del titprivado_perc['id_papel_old']
    del titprivado_perc['indice_du_mtm']
    del titprivado_perc['indice_dc_mtm']
    del titprivado_perc['ano_dt_ref2']
    del titprivado_perc['mes_dt_ref2']
    del titprivado_perc['dia_dt_ref2']
    del titprivado_perc['vertices_positivo']
    del titprivado_perc['indice_dc_dt_ref2']
    del titprivado_perc['indice_du_dt_ref2']
    del titprivado_perc['prazo_dc']
    del titprivado_perc['ano_inicio']
    del titprivado_perc['mes_inicio']
    del titprivado_perc['dia_inicio']
    del titprivado_perc['indice_du_inicio']
    del titprivado_perc['indice_dc_inicio']
    del titprivado_perc['ano_fim']
    del titprivado_perc['mes_fim']
    del titprivado_perc['dia_fim']
    del titprivado_perc['indice_du_fim']
    del titprivado_perc['indice_dc_fim']
    del titprivado_perc['dt_ref']
    del titprivado_perc['dtoperacao_mtm']
    del titprivado_perc['dif']
    del titprivado_perc['dif_mtm']
    del titprivado_perc['dif_curva']
    del titprivado_perc['pupar']
    del titprivado_perc['count_all']
    del titprivado_perc['count_0']
    del titprivado_perc['count_dif']
    del titprivado_perc['change']
    del titprivado_perc['puposnew']
    del titprivado_perc['puposicao1']
    del titprivado_perc['pu_mercado']
    del titprivado_perc['pu_curva']
    del titprivado_perc['mtm_mercado']
    del titprivado_perc['mtm_curva']
    del titprivado_perc['pu_regra_xml']
    del titprivado_perc['mtm_regra_xml']

    # Tabelas não necessárias - XML
    del titprivado_perc['id_xml_titprivado']
    del titprivado_perc['isin']
    del titprivado_perc['codativo']
    del titprivado_perc['dtemissao']
    del titprivado_perc['dtoperacao']
    del titprivado_perc['dtvencimento']
    del titprivado_perc['cnpjemissor']
    del titprivado_perc['qtdisponivel']
    del titprivado_perc['qtgarantia']
    del titprivado_perc['pucompra']
    del titprivado_perc['puvencimento']
    del titprivado_perc['puposicao']
    del titprivado_perc['puemissao']
    del titprivado_perc['principal']
    del titprivado_perc['tributos']
    del titprivado_perc['valorfindisp']
    del titprivado_perc['valorfinemgar']
    del titprivado_perc['coupom']
    del titprivado_perc['percindex']
    del titprivado_perc['caracteristica']
    del titprivado_perc['percprovcred']
    del titprivado_perc['classeoperacao']
    del titprivado_perc['idinternoativo']
    del titprivado_perc['nivelrsc']
    del titprivado_perc['header_id']
    del titprivado_perc['cusip']
    del titprivado_perc['depgar']

    # Remove as duplicatas de isin
    titprivado_perc = titprivado_perc.sort(['codigo_isin', 'id_papel', 'data_fim'], ascending=[True, True, True])
    titprivado_perc = titprivado_perc.drop_duplicates(subset=['codigo_isin', 'data_fim'])

    # titprivado_perc['data_bd'] = horario_bd
    titprivado_perc['data_bd'] = datetime.datetime.today()

    titprivado_perc = titprivado_perc.where((pd.notnull(titprivado_perc)), None)

    titprivado_perc.to_excel(save_path_titprivado_perc)

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')
    pd.io.sql.to_sql(titprivado_perc, name='mtm_renda_fixa', con=connection, if_exists='append', flavor='mysql', index=0)
    connection.close()

    ###############################################################################
    # 6 - Preenchimento tabela xml
    ###############################################################################

    del original['id_xml_titprivado']
    del original['pu_mercado']
    del original['mtm_mercado']
    del original['pu_curva']
    del original['mtm_curva']
    del original['pu_regra_xml']
    del original['mtm_regra_xml']
    del original['data_referencia']

    del titprivado['mtm']

    titprivado = titprivado.merge(finalizacao, on=['codigo_isin'], how='left')

    titprivado_xml = titprivado[titprivado.dt_ref == titprivado.data_referencia].copy()

    titprivado_xml = titprivado_xml.rename(columns={'mtm': 'mtm_calculado'})

    titprivado_xml['pu_mercado'] = np.where(titprivado_xml['caracteristica'] == 'N', titprivado_xml['mtm_calculado'], 0)
    titprivado_xml['pu_curva'] = np.where(titprivado_xml['caracteristica'] == 'V', titprivado_xml['pupar'], 0)

    titprivado_xml = titprivado_xml[['id_papel', 'pu_mercado', 'pu_curva', 'data_referencia']].copy()

    final = original.merge(titprivado_xml, on=['id_papel'], how='left')

    final['pu_mercado'] = np.where((final['pu_mercado'].isnull()) | (final['pu_mercado'] == 0), final['puposicao'],
                                   final['pu_mercado'])
    final['mtm_mercado'] = final['pu_mercado'] * (final['qtdisponivel'] + final['qtgarantia'])

    final['pu_curva'] = np.where(final['pu_curva'].isnull(), final['puposicao'], final['pu_curva'])
    final['mtm_curva'] = final['pu_curva'] * (final['qtdisponivel'] + final['qtgarantia'])

    final['pu_regra_xml'] = np.where(final['caracteristica'] == 'N', final['pu_mercado'], final['pu_curva'])
    final['mtm_regra_xml'] = np.where(final['caracteristica'] == 'N', final['mtm_mercado'], final['mtm_curva'])

    final['data_bd'] = datetime.datetime.today()

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')
    pd.io.sql.to_sql(final, name='xml_titprivado', con=connection, if_exists='append', flavor='mysql', index=0)
    connection.close()
