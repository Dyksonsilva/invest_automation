def simulacao_credito():

    import pandas as pd
    import pymysql as db
    import numpy as np
    import math
    import logging
    import random
    from dependencias.Metodos.funcoes_auxiliares import get_global_var
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from findt import FinDt
    feriados_sheet = full_path_from_database('feriados_nacionais') + 'feriados_nacionais.csv'
    cnpj_hdi = get_global_var("cnpj_hdi")
    logger = logging.getLogger(__name__)
    dt_base = get_data_ultimo_dia_util_mes_anterior()
    dt_base = dt_base[0] + '-' + dt_base[1] + '-' + dt_base[2]
    logger.info("Conectando no Banco de dados")
    connection=db.connect('localhost',user='root',passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")
    query='select distinct nome_emissor, cnpj_emissor, data_criacao_emissor from projeto_inv.bmf_emissor where cnpj_emissor>0;'
    emissor=pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")
    #Fecha conexão
    #connection.close()
    emissor=emissor.sort(['cnpj_emissor', 'data_criacao_emissor'], ascending=[True, False])
    emissor1=emissor.drop_duplicates(subset=['cnpj_emissor'], take_last=False)
    emissor1['cnpj']=emissor1['cnpj_emissor'].astype(float)
    emissor1=emissor1.rename(columns={'nome_emissor': 'contraparte'})
    emissor2=emissor1[['cnpj', 'contraparte']]
    #seleção da carteira
    def quadro_oper(dt_base,cnpj):
        #Global Variables:
        global start
        global header_id_carteira
        global quadro_oper
        global tp
        global tp_expo
        global tp_fluxo
        global id_relatorio_qo
        start = dt_base
        logger.info("Conectando no Bancode dados")
        connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
        logger.info("Conexão com DB executada com sucesso")
        query = 'select * from projeto_inv.xml_header_org where cnpjcpf="' + cnpj +'" and dtposicao='+'"'+dt_base+'";'
        df = pd.read_sql(query, con=connection)
        if len(df)==0:
            query = 'select * from projeto_inv.xml_header_org where cnpj="' + cnpj +'" and dtposicao='+'"'+dt_base+'";'
            df = pd.read_sql(query, con=connection)
        df = df.sort(['cnpj', 'cnpjcpf','data_bd'], ascending=[True, True, False])
        df = df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
        df = df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        del df['index']
        #header_id_carteira=df.get_value(0,'header_id').astype(str)
        header_id_carteira= str(2150)
        # quadro de operaçao
        query='select a.* from projeto_inv.xml_quadro_operacoes a right join (select header_id, max(data_bd) as data_bd from projeto_inv.xml_quadro_operacoes where header_id='+header_id_carteira+' group by 1) b on a.header_id=b.header_id and a.data_bd=b.data_bd;'
        quadro_oper=pd.read_sql(query, con=connection)
        logger.info("Leitura do banco de dados executada com sucesso")
        tp=quadro_oper.loc[quadro_oper['produto'].isin(['título privado','debênture'])]
        id_relatorio_qo=tp['id_relatorio_qo'][0]
        # fidc
        fundos=quadro_oper.loc[quadro_oper['produto'].isin(['fundo'])].copy()
        fundos['fundo_final']=np.where(fundos['fundo_ult_nivel'].isnull(),fundos['fundo'],fundos['fundo_ult_nivel'])
        #INCLUIR FIDCS
        fidc=fundos[fundos['fundo_final'].str.contains('FIDC|DIREITOS|CREDITÓRIO|CREDITORIOS|DIREITOS')]
        tp=tp.append(fidc)
        tp['contraparte']=np.where(tp.produto=='fundo',tp.fundo_final, tp.contraparte)
        tp['cnpj_fundo_final']=np.where((tp.produto=='fundo') & (tp.cnpjfundo_outros.isnull()),tp.cnpjfundo_1nivel,tp.cnpjfundo_outros)
        tp['cnpj']=np.where(tp.produto=='fundo',tp.cnpj_fundo_final,tp.cnpj)
        # pegar exposicao mercado
        tp['expo_final']= np.where(tp['caracteristica']!='N', tp['pu_mercado']*tp['quantidade'],tp['mtm_info'])
        tp['expo_final']= np.where(tp['expo_final']==0, tp['mtm_info'], tp['expo_final'])
        tp['expo_final']= np.where(tp.produto=='fundo', tp['mtm_info'],tp['expo_final'])
        # incluir data de vencimento de fidc
        query='select distinct codigo_isin, dtvencimento, data_bd, max(data_ref) as dt_ref from projeto_inv.mtm_renda_fixa group by 1,2,3;   '
        dfvencto=pd.read_sql(query, con=connection)
        dfvencto=dfvencto.sort_values(by=['codigo_isin','data_bd'], ascending=[True, False])
        dfvencto1=dfvencto.drop_duplicates(subset=['codigo_isin'], take_last=False)
        dfvencto1['dt_vencto_fim']=np.where(dfvencto1.dtvencimento.isnull(), dfvencto1.dt_ref, dfvencto1.dtvencimento)
        base_vencto=dfvencto1[['codigo_isin', 'dt_vencto_fim']].copy()
        tp = pd.merge(tp, base_vencto, left_on='isin', right_on='codigo_isin', how='left')
        tp['dt_vencto_1'] = np.where(tp.dt_vencto.isnull(), tp.dt_vencto_fim, tp.dt_vencto)
        del tp['codigo_isin']
        del tp['dt_vencto']
        del tp['dt_vencto_fim']
        tp = tp.rename(columns={'dt_vencto_1':'dt_vencto'})
        tp_mtm = tp[['expo_final', 'quantidade', 'isin', 'cnpj','produto', 'dt_vencto', 'fundo_final']].copy()
        tp_mtm = pd.merge(tp_mtm, emissor2, left_on='cnpj', right_on='cnpj', how='left')
        tp_mtm['contraparte']=tp_mtm['contraparte'].fillna(tp_mtm['fundo_final'])
        del tp_mtm['fundo_final']
        tp_expo=tp_mtm.groupby([ 'isin', 'cnpj','contraparte','produto', 'dt_vencto'], as_index=False).sum()
        tp_expo=tp_expo.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        tp_expo['cnpj']=tp_expo['cnpj'].astype(float)
        tp_expo['cnpj1']=""
        for i in range(0,len(tp_expo)):
            if tp_expo['cnpj'][i]>0:
                tp_expo['cnpj1'][i]=math.floor(tp_expo['cnpj'][i])
            #i=i+1
        tp_expo['cnpj2']=tp_expo['cnpj1'].astype(str).str.zfill(14)
        del tp_expo['cnpj']
        del tp_expo['cnpj1']
        tp_expo=tp_expo.rename(columns={'cnpj2':'cnpj'})
        # fluxos
        query='select a.codigo_isin, a.fv, a.data_ref, a.indexador, a.data_mtm, a.data_bd from projeto_inv.mtm_renda_fixa a right join (select codigo_isin, data_mtm, max(data_bd) as data_bd from projeto_inv.mtm_renda_fixa where data_mtm="'+dt_base+'" group by 1,2) b on a.codigo_isin=b.codigo_isin and a.data_mtm=b.data_mtm and a.data_bd=b.data_bd;'
        fluxos=pd.read_sql(query, con=connection)
        tp_fluxo=pd.merge(tp_expo, fluxos, left_on='isin', right_on='codigo_isin')
        del tp_fluxo['data_bd']
        tp_fluxo['fv1']=abs(tp_fluxo['fv']* tp_fluxo['quantidade'])
        del tp_fluxo['fv']
        tp_fluxo=tp_fluxo.rename(columns={ 'fv1': 'fv'})
        del tp_fluxo['codigo_isin']
        tp_fluxo=tp_fluxo.reindex(columns=['isin', 'data_ref', 'indexador','fv' ])
        #Fecha conexão
        connection.close()
    quadro_oper(dt_base, cnpj_hdi)
    fluxo_original = tp_fluxo.copy()
    logger.info("Conectando no Banco de dados")
    connection=db.connect('localhost',user='root',passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")
    # LGD
    query= 'select a.produto, a.lgd from projeto_inv.lgd a right join (select produto, max(data_bd) as data_bd from projeto_inv.lgd group by 1) b on a.produto=b.produto and a.data_bd=b.data_bd;'
    lgd = pd.read_sql(query, con=connection)
    #simulação de cenário____________________________________________________________________________

    # Data de 2020 limite para a criacao das tabelas
    dt_ref = pd.date_range(start=datetime.date(2017, 1, 1),
                           end=datetime.date(int(2020), int(10), 31), freq='M').date

    # Uniao das tabelas criadas com union all
    query_intermediate = '(SELECT * FROM curva_ettj_interpol UNION ALL '
    for dt in dt_ref:
        month = '0' + str(dt.month) if len(str(dt.month)) == 1 else str(dt.month)
        year = '0' + str(dt.year) if len(str(dt.year)) == 1 else str(dt.year)
        query_intermediate = query_intermediate + 'SELECT * FROM projeto_inv.curva_ettj_interpol_' + year + "_" + month + " UNION ALL "
    query_intermediate = query_intermediate[:-11] + ")"

    query='SELECT * FROM (SELECT n.* FROM ' + query_intermediate + ' n INNER JOIN (  SELECT indexador_cod, month(dt_ref ) as mes, year (dt_ref) as ano, max(dt_ref) as max  FROM ' + query_intermediate + " p GROUP BY 1,2,3) a on a.indexador_cod=n.indexador_cod and a.max=n.dt_ref ) tx WHERE dt_ref<='" +  dt_base + "';"

    cen = pd.read_sql(query, con=connection)
    cen = cen.sort(['indexador_cod', 'prazo', 'dt_ref','data_bd'], ascending=[True, True, True, False])
    cen1 = cen.drop_duplicates(subset=['indexador_cod', 'prazo', 'dt_ref'], take_last=False)
    cen1['indexador_shift']=cen1['indexador_cod'].shift()
    cen1['prazo_shift']=cen1['prazo'].shift()
    cen1['tx_spot_shift']=cen1['tx_spot'].shift()
    cen1['dif']=np.where((cen1.indexador_cod==cen1.indexador_shift) & (cen1.prazo==cen1.prazo_shift), cen1.tx_spot-cen1.tx_spot_shift, 'NaN')
    cen2 = cen1[cen1.dif != 'NaN']
    #________________________________________________________________________________________________________
    # GERACAO DE SERIE DE DIAS ÚTEIS E DIAS CORRIDOS
    ano=start[0:4]
    mes=start[5:7]
    dia=start[8:10]
    dt_inicio=dia+'-'+ mes+'-'+ano
    dt_max=max(tp_expo['dt_vencto'])
    per = FinDt.DatasFinanceiras(dt_inicio, dt_max, path_arquivo=feriados_sheet)
    du=pd.DataFrame(columns=['data_ref'])
    dc=pd.DataFrame(columns=['data_ref'])
    dc['data_ref']=per.dias()
    dc['flag_dc']=1
    du['data_ref']=per.dias(3)
    du['flag_du']=1
    serie_dias=pd.merge(dc,du, left_on=['data_ref'], right_on=['data_ref'], how='left')
    serie_dias['flag_du']=serie_dias['flag_du'].fillna(0)
    serie_dias['indice_dc'] = np.cumsum(serie_dias['flag_dc'])
    serie_dias['indice_du'] = np.cumsum(serie_dias['flag_du'])
    del serie_dias['flag_du']
    del serie_dias['flag_dc']
    # OBS: a contagem de dias é o valor do índice menos um
    #PD e matriz de  migracao________________________________
    query = "select * from projeto_inv.pd_mes;"
    pd_mes = pd.read_sql(query, con=connection)
    pd_mes = pd_mes.sort(['rtg', 'produto','prazo', 'data_bd'], ascending=[True, True, True, False])
    pd_mes = pd_mes.drop_duplicates(subset=['rtg','produto', 'prazo'], take_last=False)
    query = "select * from projeto_inv.matriz_migr_mes;"
    matriz_mes=pd.read_sql(query, con=connection)
    matriz_mes=matriz_mes.sort(['cod_rtg_de', 'cod_rtg_para', 'data_bd'], ascending=[True, True, False])
    matriz_mes=matriz_mes.drop_duplicates(subset=['cod_rtg_de', 'cod_rtg_para'], take_last=False)
    #TIPO DE PAPEL
    query= "select distinct codigo_isin, data_bd, tipo_ativo from projeto_inv.bmf_numeraca where tipo_ativo in ('DBS', 'LFI', 'LFN', 'DP', 'C', 'CC','CCC', 'CRI');"
    caracteristica=pd.read_sql(query, con=connection)
    caracteristica=caracteristica.sort(['codigo_isin', 'data_bd'], ascending=[True, False])
    caracteristica=caracteristica.drop_duplicates(subset=['codigo_isin'], take_last=False)
    del caracteristica['data_bd']
    df_original=pd.merge(tp_expo, caracteristica, left_on='isin', right_on='codigo_isin', how='left')
    df_original.ix[df_original.tipo_ativo == 'CC', 'tipo_pd']='ccb'
    df_original.ix[df_original.tipo_ativo == 'CCC', 'tipo_pd']='ccb'
    df_original.ix[df_original.tipo_ativo =='DBS', 'tipo_pd']='deb'
    df_original.ix[df_original.tipo_ativo =='C', 'tipo_pd']='cdb'
    df_original.ix[df_original.tipo_ativo =='LFI', 'tipo_pd']='cdb'
    df_original.ix[df_original.tipo_ativo =='LFN', 'tipo_pd']='cdb'
    df_original.ix[df_original.tipo_ativo =='DP', 'tipo_pd']='cdb'
    df_original['tipo_pd']=df_original['tipo_pd'].fillna('fidc')
    del df_original['codigo_isin']
    #RATING________________________________
    query='select cod_rtg, rtg from projeto_inv.de_para_rtg a right join (select max(data_bd) as data_bd from projeto_inv.de_para_rtg) b on a.data_bd = b.data_bd;'
    depara=pd.read_sql(query, con=connection)
    #regua mestra
    query='select distinct a.cod_rtg, a.agencia_rtg, a.rtg from projeto_inv.de_para_rtg a right join (select agencia_rtg, max(data_bd) as data_bd from projeto_inv.de_para_rtg where agencia_rtg="regua" group by 1) b on a.agencia_rtg=b.agencia_rtg and a.data_bd = b.data_bd;'
    regua_rtg=pd.read_sql(query, con=connection)
    del regua_rtg['agencia_rtg']
    #rating por isin
    query='select distinct a.isin, a.agencia_tipo_rtg, a.rtg from projeto_inv.rating_isin as a right join (select max(data_bd) as data_bd from projeto_inv.rating_isin where dt_ref= "'+ dt_base +'" ) as b on a.data_bd=b.data_bd;'
    rtg_isin=pd.read_sql(query, con=connection)
    rtg_local = rtg_isin.loc[rtg_isin['agencia_tipo_rtg'].isin(['RTG_MDY_NSR', 'RTG_MDY_NSR_SR_UNSECURED',  'RTG_MDY_NSR_SUBORDINATED','RTG_SP_NATIONAL','RTG_FITCH_NATIONAL_LT', 'RTG_FITCH_NATIONAL', 'RTG_FITCH_NATIONAL_SR_UNSECURED', 'RTG_FITCH_NATL_SUBORDINATED'])]
    rtg_local = pd.merge(rtg_local, depara, left_on='rtg', right_on='rtg', how='left')
    rtg_pior=rtg_local[['isin', 'cod_rtg']].copy()
    rtg_pior=rtg_pior.groupby(['isin'],as_index=False).max()
    rtg_pior=pd.merge(rtg_pior, regua_rtg, left_on='cod_rtg', right_on='cod_rtg', how='left')
    #rating por contraparte
    query='select distinct a.cnpj, a.agencia_tipo_rtg, a.rtg from projeto_inv.rating_contraparte as a right join (select max(data_bd) as data_bd from projeto_inv.rating_contraparte where dt_ref= "'+ dt_base +'" ) as b on a.data_bd=b.data_bd;'
    rtg_c=pd.read_sql(query, con=connection)
    rtg_c_local=rtg_c.loc[rtg_c['agencia_tipo_rtg'].isin(['RTG_MDY_NSR_ISSUER','RTG_SP_NATIONAL_LT_ISSUER_CREDIT','RTG_FITCH_NATIONAL_LT','RTG_FITCH_NATIONAL_SR_UNSECURED' ])]
    rtg_c_local=pd.merge(rtg_c_local, depara, left_on='rtg', right_on='rtg', how='left')
    rtg_c_pior=rtg_c_local[['cnpj', 'cod_rtg']].copy()
    rtg_c_pior=rtg_c_pior.groupby(['cnpj'], as_index=False).max()
    rtg_c_pior=pd.merge(rtg_c_pior, regua_rtg, left_on='cod_rtg', right_on='cod_rtg', how='left')
    #agregar o rtg na base
    df_original=pd.merge(df_original, rtg_pior, left_on='isin', right_on='isin', how='left')
    df_original=df_original.rename(columns={'cod_rtg':'cod_rtg_isin', 'rtg':'rtg_isin'})
    df_original=pd.merge(df_original, rtg_c_pior, left_on='cnpj', right_on='cnpj', how='left')
    df_original=df_original.rename(columns={'cod_rtg':'cod_rtg_cnpj', 'rtg':'rtg_cnpj'})
    df_original['cod_rtg']=np.where(df_original['cod_rtg_isin'].isnull(),df_original.cod_rtg_cnpj,df_original.cod_rtg_isin)
    del df_original['cod_rtg_isin']
    del df_original['cod_rtg_cnpj']
    del df_original['rtg_isin']
    del df_original['rtg_cnpj']
    df_original = pd.merge(df_original, regua_rtg, left_on='cod_rtg', right_on='cod_rtg', how='left')
    #assumir rtg padrão missing: 'Aa3' e cod_rtg=4
    df_original['cod_rtg']=df_original['cod_rtg'].fillna(3)
    df_original['rtg']=df_original['rtg'].fillna('Aa2')
    df_original=df_original.rename(columns={'rtg': 'rating'})
    #Fecha conexão
    #connection.close()
    #inicio da simulacao
    #def simulacao(id_simulacao):
    id_simulacao = 0
    global df
    global fluxo
    global df_resultado
    global i_tempo
    df=df_original.copy()
    fluxo=fluxo_original.copy()
    #criar a serie de tempo mensal - para simulacao
    end=max(df['dt_vencto'])
    end=end+pd.DateOffset(months=2)
    inicio=pd.to_datetime(start)+pd.DateOffset(months=1)
    tempo=pd.DataFrame()
    tempo['serie']=pd.date_range(inicio, end, freq='M')
    tempo['serie']=tempo['serie'].dt.date
    df=df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    df['dt_base']=start
    #primeira rodada
    df['rtg']=df['rating']
    fim=pd.DataFrame()
    #### simulacao
    i_tempo=0
    ''''
    while len(df)>0:
        np.random.seed(1234+i_tempo+id_simulacao)
        df['aleat_matriz'] = np.random.uniform(0,1,len(df))
        df['aleat_pd'] = np.random.uniform(0,1,len(df))
        df_matriz=pd.merge(df, matriz_mes, left_on=['rtg'], right_on=['rtg_de'],how='left')
        df_rtg=df_matriz[(df_matriz['prob_ini']<df_matriz['aleat_matriz']) & (df_matriz['aleat_matriz']<=df_matriz['prob_fim'])]
        df_rtg['rtg']=df_rtg['rtg_para']
        df_rtg['dt_ref']=tempo['serie'][i_tempo]
        #  prazo=1 -> simulação mensal
        df_rtg['prazo']=1
        df_rtg1=pd.merge(df_rtg, pd_mes, left_on=['tipo_pd', 'prazo', 'rtg'], right_on=['produto', 'prazo','rtg'], how='left')
        df_rtg1['default']=np.where(df_rtg1.pd_mensal>df_rtg1.aleat_pd, 1, 0)
        df_rtg_mes=df_rtg1[['isin', 'cnpj','contraparte', 'dt_vencto', 'tipo_ativo', 'tipo_pd', 'rating','rtg', 'dt_ref', 'prazo', 'dt_base', 'pd_mensal' , 'default']].copy()
        df_rtg_mes['break']=np.where((df_rtg_mes['default']==1) | (pd.to_datetime(df_rtg_mes['dt_vencto'])<=pd.to_datetime(df_rtg_mes['dt_ref'])),1,0)
        default_break=df_rtg_mes[df_rtg_mes['break']==1].copy()
        nao_default=df_rtg_mes[~(df_rtg_mes['break']==1)].copy()
        fim=fim.append(default_break)
        df=nao_default[['isin', 'cnpj', 'contraparte','dt_vencto', 'tipo_ativo', 'tipo_pd', 'rating','rtg', 'dt_base' ]].copy()
        i_tempo=i_tempo+1
    #fim=fim.rename(columns={'dt_ref': 'dt_default'})
    #fim['lgd']=0
    base_default=fim[fim['default']==1].copy()
    base_nao_default=fim[fim['default']==0].copy()
    base_default=base_default.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    del base_default['index']
    for i_lgd in range(0, len(base_default)):
        np.random.seed(123+i_lgd+id_simulacao)
        lgd_base_0=lgd[lgd['produto']==base_default['tipo_pd'][i_lgd]]
        lgd_base=lgd_base_0.copy()
        lgd_base['aleat_lgd'] = np.random.uniform(0,1,len(lgd_base))
        lgd_base=lgd_base.sort(['aleat_lgd'])
        lgd_base=lgd_base.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        lgd_perc=lgd_base.get_value(0,'lgd')
        base_default.loc[i_lgd, 'lgd']=lgd_perc
        ##VALIDAR ESTA ITERAÇÃO:
        #i_lgd=i_lgd+1
    '''
    #df_fim = base_nao_default.append(base_default)
    fluxo.head()
    df.head()
    df_fluxo=pd.merge(df, fluxo, left_on=['isin'], right_on=['isin'], how='left')
    df_fluxo=pd.merge(df_fluxo, serie_dias, left_on=['data_ref'], right_on=['data_ref'], how='left')
    df_fluxo=df_fluxo.rename(columns={'indice_du': 'du_ref', 'indice_dc':'dc_ref'})
    #df_fluxo=pd.merge(df_fluxo, serie_dias, left_on=['dt_default'], right_on=['data_ref'], how='left')
    #del df_fluxo['data_ref_y']
    df_fluxo=df_fluxo.rename(columns={'indice_du': 'du_default', 'indice_dc':'dc_default', 'data_ref_x': 'data_ref'})
    #___________________________CENARIO_______________________________________________
    base_dt=pd.DataFrame(columns=['dt', 'aleat'])
    base_dt['dt']=cen2['dt_ref'].unique()
    np.random.seed(int(random.random()*1000000))
    base_dt['aleat'] = np.random.uniform(0,1,len(base_dt))
    base_dt=base_dt.sort(['aleat'])
    base_dt=base_dt.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    base_dt=base_dt[['dt', 'aleat']]
    dt_max=max(base_dt.dt)
    base_ref_0=cen2[cen2.dt_ref==dt_max]
    base_ref=base_ref_0.copy()
    del base_ref['dif']
    dt=base_dt.get_value(0,'dt')
    #erro
    erro_0=cen2[cen2.dt_ref==dt]
    erro=erro_0.copy()
    erro=erro[['indexador_cod', 'prazo', 'dt_ref','dif']]
    del erro['dt_ref']
    base_ref=pd.merge(base_ref,erro, how='left', left_on=['indexador_cod', 'prazo'], right_on=['indexador_cod', 'prazo'])
    base_ref['tx_spot_sim']=base_ref['tx_spot'].astype(float)+ base_ref['dif'].astype(float)
    del base_ref['indexador_shift']
    del base_ref['prazo_shift']
    del base_ref['tx_spot_shift']
    del base_ref['dif']
    #--fluxo e cenário
    df_fluxo['indexador_cod']=""
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='DI1', 'CDI' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='CDI', 'CDI' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IAP', 'DIC' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IPC', 'DIC' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IGM', 'DIM' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='PRE', 'PRE' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='TR', 'TP' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IGP', 'DIM' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IPCA', 'DIC' ,df_fluxo['indexador_cod'])
    df_fluxo['indexador_cod']=np.where(df_fluxo.indexador =='IGPM', 'DIM' ,df_fluxo['indexador_cod'])
    cenario=base_ref[['prazo', 'indexador_cod', 'tx_spot_sim']].copy()
    df_fluxo_sim=pd.merge(df_fluxo, cenario, left_on=['du_ref','indexador_cod'], right_on=['prazo','indexador_cod'], how='left')
    df_fluxo_sim['tx_spot_sim']=df_fluxo_sim['tx_spot_sim'].fillna(0)
    df_fluxo_sim['pv']=df_fluxo_sim['fv']/(1+df_fluxo_sim['tx_spot_sim'])

    #### ate aqui igual ao script anterior...

    # pd_acum
    query = "select * from projeto_inv.pd_acum;"
    pd_acum = pd.read_sql(query, con=connection)
    pd_acum = pd_acum.sort(['rtg','prazo', 'data_bd'], ascending=[True, True, False])
    pd_acum = pd_acum.drop_duplicates(subset=['rtg', 'prazo'], take_last=False)

    ## Calculo de duration
    #df_fluxo_sim_original = df_fluxo_sim.copy()
    #df_fluxo_sim = df_fluxo_sim_original.copy()

    del df_fluxo_sim['level_0']
    del df_fluxo_sim['index']
    del df_fluxo_sim['quantidade']
    del df_fluxo_sim['cod_rtg']
    del df_fluxo_sim['rating']
    del df_fluxo_sim['fv']
    del df_fluxo_sim['prazo']
    del df_fluxo_sim['tx_spot_sim']

    ## Exemplo da Localiza para debug...
    #df_fluxo_sim = df_fluxo_sim[df_fluxo_sim['contraparte']=='LOCALIZA RENT A CAR SA']

    df_fluxo_sim = df_fluxo_sim.loc[df_fluxo_sim['pv'] != 0]

    df_fluxo_sim['du_ref_times_pv'] = df_fluxo_sim['du_ref'] * df_fluxo_sim['pv']

    df_fluxo_sim_group = df_fluxo_sim[['isin', 'du_ref_times_pv', 'pv']]
    df_fluxo_sim_group = df_fluxo_sim_group.groupby(['isin'], as_index=False).sum()

    dias_uteis = 22

    df_fluxo_sim_group['duration'] = round(df_fluxo_sim_group['du_ref_times_pv']/df_fluxo_sim_group['pv']/dias_uteis, 0)

    # Se duration maior que 60, recebe 60
    max_duration = get_global_var('max_duration')
    df_fluxo_sim_group['duration'] = np.where(df_fluxo_sim_group['duration'] > max_duration, max_duration, df_fluxo_sim_group['duration'])

    df_fluxo_sim = pd.merge(df_fluxo_sim, df_fluxo_sim_group[['isin', 'duration']], left_on=['isin'], right_on=['isin'], how='left')

    ## inicia def pd calculo
    df_fluxo_sim = pd.merge(df_fluxo_sim, pd_acum[['rtg', 'prazo', 'pd_acum']],
                                     left_on=['rtg', 'duration'],
                                     right_on=['rtg', 'prazo'],
                                     how='left')

    df_fluxo_sim_group = pd.merge(df_fluxo_sim_group,
                                           df_fluxo_sim[['isin', 'pd_acum', 'contraparte', 'tipo_pd']].groupby(['isin'], as_index=False).max(),
                                           left_on=['isin'], right_on=['isin'], how='left')

    df_fluxo_sim_group['lgd'] = 0.0

    # Pega lgds aleatorias para calculo
    for row in range(0, len(df_fluxo_sim_group)):

        lgd_sample = pd.DataFrame()

        np.random.seed(int(random.random() * 1000000))

        lgd_random = lgd.sample(len(lgd), random_state=int(random.random()*1000000))

        lgd_random[lgd_random['produto'] == 'cdb'].iloc[0]

        lgd_sample = lgd_sample.append(lgd_random[lgd_random['produto'] == 'cdb'].iloc[0], 'cdb')

        lgd_random[lgd_random['produto'] == 'fidc'].iloc[0]

        lgd_sample = lgd_sample.append(lgd_random[lgd_random['produto'] == 'fidc'].iloc[0], 'fidc')

        lgd_random[lgd_random['produto'] == 'ccb'].iloc[0]

        lgd_sample = lgd_sample.append(lgd_random[lgd_random['produto'] == 'ccb'].iloc[0], 'ccb')

        lgd_random[lgd_random['produto'] == 'deb'].iloc[0]

        lgd_sample = lgd_sample.append(lgd_random[lgd_random['produto'] == 'deb'].iloc[0], 'deb')

        df_fluxo_sim_group['lgd'].iloc[row] = lgd_sample[lgd_sample['produto'] == df_fluxo_sim_group['tipo_pd'].iloc[row]]['lgd'].iloc[0]

    df_fluxo_sim_group['pv_times_pd_acum'] = df_fluxo_sim_group['pv'] * df_fluxo_sim_group['pd_acum']
    df_fluxo_sim_group['pv_times_lgd'] = df_fluxo_sim_group['pv'] * df_fluxo_sim_group['lgd']

    df_fluxo_sim_contraparte = df_fluxo_sim_group[['contraparte', 'pv_times_pd_acum', 'pv_times_lgd', 'pv']].groupby(['contraparte'], as_index=False).sum()

    df_fluxo_sim_contraparte['pd_sim'] = df_fluxo_sim_contraparte['pv_times_pd_acum']/df_fluxo_sim_contraparte['pv']
    df_fluxo_sim_contraparte['lgd_sim'] = df_fluxo_sim_contraparte['pv_times_lgd']/df_fluxo_sim_contraparte['pv']

    df_fluxo_sim_contraparte['perda_perc'] = 0.0
    df_fluxo_sim_contraparte['pd_vs_rand'] = 0.0

    # Executa script para 100000 simulacoes
    number_of_simulation = get_global_var('number_of_simulations')

    # Calcula simulacao por contraparte
    for row in range(0, len(df_fluxo_sim_contraparte)):

        np.random.seed(int(random.random() * 1000000))

        df_simulation = pd.DataFrame(np.random.uniform(0, 1, size=number_of_simulation))

        df_simulation['pd_sim'] = df_fluxo_sim_contraparte['pd_sim'].iloc[row]

        df_simulation = df_simulation.rename(columns={0: 'rand'})

        df_simulation['pd_vs_rand'] = np.where(df_simulation['pd_sim'] > df_simulation['rand'], 1, 0)

        df_fluxo_sim_contraparte['pd_vs_rand'].iloc[row] = df_simulation['pd_vs_rand'].mean()

        df_fluxo_sim_contraparte['perda_perc'].iloc[row] = df_simulation['pd_vs_rand'].mean() * df_fluxo_sim_contraparte['lgd_sim'].iloc[row]

    df_fluxo_sim_contraparte = pd.merge(df_fluxo_sim_contraparte,
                                         df_fluxo_sim[['contraparte', 'tipo_ativo']].groupby(['contraparte'], as_index=False).max(),
                                         left_on=['contraparte'],
                                         right_on=['contraparte'],
                                         how='left')

    df_fluxo_sim_contraparte['recuperacao']=np.where(df_fluxo_sim_contraparte.tipo_ativo=='DP', 20000000, 0)

    #df_fluxo_sim_contraparte['pv_perda'] = df_fluxo_sim_contraparte['perda_perc'] * df_fluxo_sim_contraparte['pv']

    df_fluxo_sim_contraparte['pv_perda'] = (df_fluxo_sim_contraparte['perda_perc'] * df_fluxo_sim_contraparte['pv'] - df_fluxo_sim_contraparte['recuperacao']).clip(0,None)

    hoje = pd.datetime.today()

    df_fluxo_sim_contraparte['data_bd']=hoje
    df_fluxo_sim_contraparte['id_relatorio_qo']=id_relatorio_qo
    df_fluxo_sim_contraparte['dt_base'] = dt_base


    del df_fluxo_sim_contraparte['pv_times_pd_acum']
    del df_fluxo_sim_contraparte['pv_times_lgd']
    del df_fluxo_sim_contraparte['pd_sim']
    del df_fluxo_sim_contraparte['lgd_sim']
    del df_fluxo_sim_contraparte['pd_vs_rand']
    del df_fluxo_sim_contraparte['recuperacao']
    del df_fluxo_sim_contraparte['pv']
    del df_fluxo_sim_contraparte['tipo_ativo']

    df_fluxo_sim_final = pd.merge(df_fluxo_sim_contraparte,
                                 df_fluxo_sim_group[['isin', 'contraparte', 'pv', 'duration']],
                                 left_on='contraparte', right_on='contraparte', how='right')

    #Salvar no MySQL
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")
    pd.io.sql.to_sql(df_fluxo_sim_final, name='simulacao_credito_consolidado', con=connection, if_exists='append', flavor='mysql', index=0)

    #Fecha conexão
    connection.close()
