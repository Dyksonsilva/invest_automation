def titpublico_final_pv_ettj():

    import pandas as pd
    import pymysql as db
    import numpy as np
    import datetime
    import logging
    from findt import FinDt
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    logger = logging.getLogger(__name__)

    #----Declaração de constantes

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()

    # Auxiliar para pegar vna existente na base - versão dummie
    dt_vna = datetime.date(2016, 11, 29)
    # Quando tiver vna da data de posicao
    # dt_vna = dt_base

    dt_base = datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2]))

    # Diretório de save de planilhas
    save_path_tpf_fluxo_final = full_path_from_database('get_output_quadro419') + 'tpf_fluxo_final.xlsx'

    feriados_sheet = full_path_from_database('feriados_nacionais') + 'feriados_nacionais.csv'

    #----Leitura do HEADER para pegar a data de referencia do relatório

    #Informações XML
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv'
, use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'select * from projeto_inv.xml_header_org'
    xml_header = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    xml_header = xml_header[xml_header.dtposicao == dt_base].copy()
    xml_header = xml_header[xml_header.data_bd == max(xml_header.data_bd)].copy()

    horario_bd = datetime.datetime.today()

    #----Seleção da tabela xml_titpublico

    # Seleção da tabela de TITPUBLICO
    query = 'SELECT * FROM projeto_inv.xml_titpublico_org'
    tpf_xml = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    aux = xml_header[['dtposicao','header_id']].copy()
    aux['marcador'] = 1

    #Seleção da carga mais recente referente à data base do relatório
    tpf_xml = tpf_xml.merge(aux,on=['header_id'],how='left')
    tpf_xml = tpf_xml[tpf_xml.marcador==1]
    tpf_xml = tpf_xml[tpf_xml.data_bd==max(tpf_xml.data_bd)]

    #Mudança de formato
    tpf_xml['codativo'] = tpf_xml['codativo'].fillna(0)
    tpf_xml['codativo'] = tpf_xml['codativo'].astype(int)

    del tpf_xml['data_bd']
    del tpf_xml['marcador']
    del tpf_xml['pu_mercado']
    del tpf_xml['mtm_mercado']
    del tpf_xml['pu_curva']
    del tpf_xml['mtm_curva']
    del tpf_xml['pu_regra_xml']
    del tpf_xml['mtm_regra_xml']
    del tpf_xml['data_referencia']

    #----Marcação na curva

    #Seleção dos inputs e chaves
    tpf_curva = tpf_xml[['codativo','dtvencimento','dtposicao','dtoperacao','coupom','id_papel']].copy()

    #Mudanças necessárias de formato
    tpf_curva['coupom'] = tpf_curva['coupom'].astype(float)

    #Data de referência da posição
    tpf_curva['data_posicao_curva'] = dt_base

    fluxo = pd.read_sql('select * from projeto_inv.anbima_fluxo_tpf;', con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    fluxo = fluxo.sort(['titulo','dt_vencto', 'dt_ref', 'data_bd'], ascending=[True, True, True, False])
    fluxo = fluxo.drop_duplicates(subset=['titulo', 'dt_vencto', 'dt_ref'], take_last=False)
    fluxo = fluxo.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    fluxo = fluxo.rename(columns={'dt_vencto':'dtvencimento','cod_selic':'codativo'})

    #Retirada de colunas não utilizadas
    del fluxo['titulo']
    del fluxo['dt_emissao']
    del fluxo['cupom']
    del fluxo['dt_vencto2']
    del fluxo['data_bd']

    #Seleção do fluxo futuro à data da posição
    fluxo = fluxo[fluxo.dt_ref>=dt_base].copy()

    #Chamada de valores de VNA
    query = 'SELECT * FROM projeto_inv.anbima_vna'
    vna_virgem = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    vna = vna_virgem.sort(['codigo_selic','data_referencia','data_bd'], ascending=[True, False, False])
    vna = vna.drop_duplicates(subset=['codigo_selic','data_referencia'], take_last=False)
    vna = vna.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    vna = vna.rename(columns={'codigo_selic':'codativo'})

    #Mudança de formato

    #Seleção vna na data da posição
    vna = vna[vna.data_referencia==dt_vna].copy()

    #Retirada de colunas não utilizadas
    del vna['id_anbima_vna']
    del vna['titulo']
    del vna['data_referencia']
    del vna['data_bd']

    #Agregação do vna à tabela fluxo
    fluxo = fluxo.merge(vna,on=['codativo'],how='left')
    fluxo['vna'] = fluxo['vna'].fillna(1000.0)

    #Agregaçào do fluxo à tabela tpf_curva
    tpf_curva = pd.merge(tpf_curva,fluxo,right_on=['codativo','dtvencimento'], left_on=['codativo','dtvencimento'], how='left')

    #Criação da lista de datas para trazer o fv a vp
    #Começa a contar a partir da data da posição
    dt_min = dt_base
    tpf_curva['dt_ref']=pd.to_datetime(tpf_curva['dt_ref'])
    dt_max = max(tpf_curva['dt_ref'])
    dt_max = dt_max.to_datetime()
    dt_max=dt_max.date()

    dt_ref = pd.date_range(start=dt_min, end=dt_max, freq='D').date
    serie_dias = pd.DataFrame(columns=['dt_ref'])
    serie_dias['dt_ref'] = dt_ref

    per = FinDt.DatasFinanceiras(dt_min, dt_max, path_arquivo=feriados_sheet)
    du = pd.DataFrame(columns=['dt_ref'])
    du['dt_ref'] = per.dias(3)
    du['du_flag'] = 1

    serie_dias = serie_dias.merge(du,on=['dt_ref'],how='left')
    serie_dias['du_flag'] = serie_dias['du_flag'].fillna(0)
    serie_dias['prazo_du'] = np.cumsum(serie_dias['du_flag']) - 1

    del serie_dias['du_flag']

    #Agregação da tabela serie_dias à tpf_curva
    tpf_curva['dt_ref'] = tpf_curva['dt_ref'].dt.date
    tpf_curva = tpf_curva.merge(serie_dias,on=['dt_ref'],how='left')

    #Cálculo do pv
    tpf_curva['pv'] = tpf_curva['fv']*tpf_curva['vna']/(1+tpf_curva['coupom']/100)**(tpf_curva['prazo_du']/252)
    tpf_curva['pv'] = np.where(tpf_curva['codativo']==210100,tpf_curva['fv']*tpf_curva['vna'],tpf_curva['pv'])

    #Cálculo da marcação na curva
    y = tpf_curva[['id_papel','pv']].groupby(['id_papel']).agg(['sum'])
    y = y.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    y1 = pd.DataFrame(columns=['id_papel','pu_curva'])
    y1['id_papel'] = y['id_papel']
    y1['pu_curva'] = y['pv']

    tpf_curva = tpf_curva.merge(y1,on=['id_papel'],how='left')

    #Seleção das colunas necessárias
    tpf_curva = tpf_curva[['id_anbima_fluxo_tpf','id_papel','codativo','dtvencimento','dtoperacao','data_posicao_curva','pu_curva','fv','prazo_du']].copy()

    #----Informações de mercado
    #--Cria a base de valores marcados a mercado
    query = 'SELECT * FROM projeto_inv.anbima_tpf'
    base_anbima_virgem = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    base_anbima = base_anbima_virgem[base_anbima_virgem.dt_referencia<=dt_base]
    base_anbima = base_anbima[['cod_selic','dt_vencto','pu','dt_referencia','dt_carga','tx_indicativa']].copy()
    base_anbima = base_anbima.sort(['cod_selic','dt_vencto','dt_referencia','dt_carga'],ascending=[True,True,False,False])
    base_anbima = base_anbima.drop_duplicates(subset=['cod_selic','dt_vencto'],take_last=False)
    base_anbima = base_anbima[base_anbima.dt_referencia==dt_base].copy()
    base_anbima = base_anbima.rename(columns = {'cod_selic':'codativo','dt_vencto':'dtvencimento','dt_referencia':'data_referencia','pu':'pu_mercado'})

    del base_anbima['dt_carga']

    ###############################################################################
    #----União das colunas de marcação a mercado e marcação na curva
    ###############################################################################

    tpf = tpf_curva.merge(base_anbima,on=['codativo','dtvencimento'],how='left')

    del tpf['codativo']
    del tpf['dtvencimento']
    del tpf['dtoperacao']

    tpf_xml = tpf_xml.merge(tpf,on=['id_papel'],how='left')

    tpf_full = tpf_xml.copy()

    tpf_xml = tpf_xml.drop_duplicates(subset=['id_papel'])

    #Seção Compromisso
    tpf_xml['pu_mercado'] = np.where(tpf_xml['dtretorno'].notnull(),tpf_xml['puposicao'],tpf_xml['pu_mercado'])
    tpf_xml['pu_curva'] = np.where(tpf_xml['dtretorno'].notnull(),tpf_xml['puposicao'],tpf_xml['pu_curva'])

    tpf_xml['mtm_curva'] = tpf_xml['pu_curva']*(tpf_xml['qtdisponivel'] + tpf_xml['qtgarantia'])
    tpf_xml['mtm_mercado'] = tpf_xml['pu_mercado']*(tpf_xml['qtdisponivel'] + tpf_xml['qtgarantia'])

    tpf_xml['pu_regra_xml'] = np.where(tpf_xml['caracteristica']=='V',tpf_xml['pu_curva'],tpf_xml['pu_mercado'])
    tpf_xml['mtm_regra_xml'] = np.where(tpf_xml['caracteristica']=='V',tpf_xml['mtm_curva'],tpf_xml['mtm_mercado'])

    tpf_xml['data_bd'] = horario_bd

    del tpf_xml['id_xml_titpublico']
    del tpf_xml['dtposicao']
    del tpf_xml['data_posicao_curva']
    del tpf_xml['fv']
    del tpf_xml['prazo_du']
    del tpf_xml['id_anbima_fluxo_tpf']
    del tpf_xml['tx_indicativa']

    logger.info("Salvando base de dados - xml_titpublico")
    pd.io.sql.to_sql(tpf_xml, name='xml_titpublico', con=connection, if_exists='append', flavor='mysql', index=0)

    #----Trazer fv das LTN a vp, calcular o perc_mtm , DV100 e Duration
    #Fator de desconto
    dia = str(dt_base.day)
    if len(dia) == 1:
        dia = '0'+dia
    mes = str(dt_base.month)
    if len(mes) == 1:
        mes = '0'+mes
    ano = str(dt_base.year)

    #dt_base_str = ano + '-' + mes + '-' + dia

    tpf_full['indexador'] = np.where(tpf_full['indexador']=='IAP','IPCA',tpf_full['indexador'])
    tpf_full['indexador'] = np.where(tpf_full['indexador']=='IGP','IGP',tpf_full['indexador'])
    tpf_full['indexador'] = np.where(tpf_full['codativo'].isin([760100,760199]),'IPCA',tpf_full['indexador'])
    tpf_full['indexador'] = np.where(tpf_full['codativo'].isin([770100]),'IGP',tpf_full['indexador'])
    tpf_full['indexador'] = np.where(tpf_full['codativo'].isin([210100]),'PRE',tpf_full['indexador'])

    tpf_pv = tpf_full.copy()

    #Cálculo dos fatores de desconto, normal e estressado
    dt_base = str(dt_base)
    query = 'SELECT * FROM projeto_inv.curva_ettj_interpol_' + ano + '_' + mes + ' where day(dt_ref) = ' + dia + ' AND indexador_cod in("PRE","DIM","DIC");'
    ettj = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #Seleciona a última carga
    ettj = ettj.sort(['indexador_cod','dt_ref', 'data_bd'], ascending = [True,False,False])
    ettj = ettj.drop_duplicates(subset=['prazo','tx_spot', 'tx_spot_ano','tx_termo_dia','indexador_cod'], take_last=False)
    ettj['indexador']=np.where(ettj['indexador_cod']=='DIC','IPCA',np.where(ettj['indexador_cod']=='DIM', 'IGP',np.where(ettj['indexador_cod']=='SLP','SEL','PRE')))
    ettj=ettj.rename(columns={'prazo': 'prazo_du'})
    ettj_filtro=ettj[['prazo_du', 'tx_spot', 'tx_spot_ano', 'indexador']]

    tpf_pv = tpf_pv[tpf_pv.prazo_du>=0].copy()
    tpf_pv = tpf_pv.merge(ettj_filtro,on=['indexador','prazo_du'],how='left')
    tpf_pv['tx_spot'] =  tpf_pv['tx_spot'].fillna(0)
    tpf_pv['tx_spot_ano'] =  tpf_pv['tx_spot_ano'].fillna(0)
    tpf_pv['fator_desconto'] = 1/(1+tpf_pv['tx_spot'])
    tpf_pv['fator_desconto_DV100'] = 1/(1+tpf_pv['tx_spot_ano']+0.01)**(tpf_pv['prazo_du']/252)
    tpf_pv['pv'] = tpf_pv['fv']*tpf_pv['fator_desconto']
    tpf_pv['pv_DV100'] = tpf_pv['fv']*tpf_pv['fator_desconto_DV100']

    total_pv = tpf_pv[['id_papel', 'pv','fv']].copy()
    total_pv = total_pv.groupby(['id_papel']).sum().reset_index()
    total_pv = total_pv.rename(columns={'pv': 'pv_sum','fv':'fv_sum'})

    tpf_pv = tpf_pv.merge(total_pv, on=['id_papel'], how='left')
    tpf_pv['perc_mtm'] = tpf_pv['pv']/tpf_pv['pv_sum']
    tpf_pv['perc_mtm_DV100'] = tpf_pv['pv_DV100']/tpf_pv['pv_sum']

    #Cálculo do DV100
    tpf_pv['DV100_fluxo'] = (tpf_pv['perc_mtm'] - tpf_pv['perc_mtm_DV100'])*tpf_pv['pu_mercado']

    DV100 = tpf_pv[['id_papel', 'DV100_fluxo']].copy()
    DV100 = DV100.groupby(['id_papel']).sum().reset_index()
    DV100 = DV100.rename(columns={'DV100_fluxo':'DV100'})
    tpf_pv = tpf_pv.merge(DV100, on=['id_papel'], how='left')

    #Cálculo da Duration
    tpf_pv['fluxo_ponderado'] = tpf_pv['perc_mtm']*tpf_pv['prazo_du']

    duration = tpf_pv[['id_papel', 'fluxo_ponderado']].copy()
    duration = duration.groupby(['id_papel']).sum().reset_index()
    duration = duration.rename(columns={'fluxo_ponderado':'duration'})
    tpf_pv = tpf_pv.merge(duration, on=['id_papel'], how='left')

    #Normalizando para encaixar com as colunas de titprivado
    tpf_pv['fluxo_ponderado'] = tpf_pv['fluxo_ponderado']*tpf_pv['pu_mercado']
    tpf_pv['fv'] = tpf_pv['fv']*tpf_pv['pu_mercado']/tpf_pv['fv_sum']

    del tpf_pv['pv_sum']
    del tpf_pv['fv_sum']
    del tpf_pv['DV100_fluxo']
    del tpf_pv['perc_mtm_DV100']
    del tpf_pv['tx_indicativa']

    tpf_pv = tpf_pv.rename(columns={'isin':'codigo_isin','pu_mercado':'mtm','data_fim':'dt_ref'})
    tpf_pv['data_mtm'] = dt_base
    tpf_pv['data_negociacao'] = tpf_pv['data_referencia']
    tpf_pv['ano_mtm'] = dt_base[0:4]
    tpf_pv['mes_mtm'] = dt_base[5:7]
    tpf_pv['dia_mtm'] = dt_base[8:10]
    tpf_pv['indexador'] = np.where(tpf_pv['codativo'].isin([210100]),'SEL',tpf_pv['indexador'])

    #Seleção das colunas finais
    tpf_pv = tpf_pv[['id_anbima_fluxo_tpf',
                     'id_papel',
                     'codigo_isin',
                     'codativo',
                     'dtvencimento',
                     'prazo_du',
                     'fv',
                     'indexador',
                     'tx_spot',
                     'fator_desconto',
                     'fator_desconto_DV100',
                     'pv',
                     'pv_DV100',
                     'perc_mtm',
                     'DV100',
                     'fluxo_ponderado',
                     'duration',
                     'mtm',
                     'data_mtm',
                     'ano_mtm',
                     'mes_mtm',
                     'dia_mtm',
                     'data_negociacao']].copy()


    #Cria o flag de tipo_ativo T(ítulo) P(úblico) F(eferal)
    tpf_pv['tipo_ativo'] = 'TPF'
    tpf_pv['data_bd'] = horario_bd
    tpf_pv = tpf_pv.sort(['codigo_isin','codativo','prazo_du'],ascending=[True,True,True])
    tpf_pv = tpf_pv.drop_duplicates(subset=['codigo_isin','codativo','dtvencimento','prazo_du'],take_last=True)

    #Coloca na base mtmt_renda_fixa

    logger.info("Salvando base de dados - mtm_renda_fixa")
    pd.io.sql.to_sql(tpf_pv, name='mtm_renda_fixa', con=connection,if_exists="append", flavor='mysql', index=0)

    #Fecha conexão
    connection.close()

    tpf_pv.to_excel(save_path_tpf_fluxo_final)
