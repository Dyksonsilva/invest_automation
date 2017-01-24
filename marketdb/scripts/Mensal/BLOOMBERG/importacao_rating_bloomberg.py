def importacao_rating_bloomberg():
    import pandas as pd
    import pymysql as db
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dt_ref = dtbase[0] + '-' + dtbase[1] + '-' + dtbase[2]

    data_bd = pd.datetime.today()

    # Define as variáveis de input e output dos arquivos
    worksheet_path = full_path_from_database("bloomberg_ratings_input") + 'levantamento de contrapartes '+dt_ref+'.xlsx'
    save_path = full_path_from_database("bloomberg_ratings_output")

    bancos = pd.read_excel(worksheet_path, sheetname='bancos_sem_Link',skiprows=1, header=0)

    contraparte_rtg = pd.DataFrame(columns=['cnpj',	'contraparte','id_bloomberg', 'issuer','agencia_tipo_rtg', 'rtg', 'dt_ref','data_bd'])

    n = len(bancos)
    i = 0
    i1 = 0
    for i in range(0, n):
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 'RTG_FITCH_ST_DEBT_LC', bancos['RTG_FITCH_ST_DEBT_LC'][i], dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_LT_ISSUER_DEFAULT'	, bancos['RTG_FITCH_LT_ISSUER_DEFAULT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_OUTLOOK'	, bancos['RTG_FITCH_OUTLOOK'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_ST_LC_DEBT'	, bancos['RTG_MDY_ST_LC_DEBT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_LC_CURR_ISSUER_RATING'	, bancos['RTG_MDY_LC_CURR_ISSUER_RATING'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_OUTLOOK'	, bancos['RTG_MDY_OUTLOOK'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_SP'	, bancos['RTG_SP'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_SP_ST_LC_ISSUER_CREDIT'	, bancos['RTG_SP_ST_LC_ISSUER_CREDIT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MOODY'	, bancos['RTG_MOODY'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MOODY_NO_WATCH'	, bancos['RTG_MOODY_NO_WATCH'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MOODY_LONG_TERM'	, bancos['RTG_MOODY_LONG_TERM'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MOODY_LONG_ISSUE_LEVEL'	, bancos['RTG_MOODY_LONG_ISSUE_LEVEL'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MOODY_DES'	, bancos['RTG_MOODY_DES'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_OUTLOOK'	, bancos['RTG_MDY_OUTLOOK'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_SEN_UNSECURED_DEBT'	, bancos['RTG_MDY_SEN_UNSECURED_DEBT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_INITIAL'	, bancos['RTG_MDY_INITIAL'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_LC_CURR_ISSUER_RATING'	, bancos['RTG_MDY_LC_CURR_ISSUER_RATING'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_LT_CORP_FAMILY'	, bancos['RTG_MDY_LT_CORP_FAMILY'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_ISSUER'	, bancos['RTG_MDY_NSR_ISSUER'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR'	, bancos['RTG_MDY_NSR'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR'	, bancos['RTG_MDY_NSR'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_ISSUER'	, bancos['RTG_MDY_NSR_ISSUER'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_SHORT_TERM'	, bancos['RTG_MDY_NSR_SHORT_TERM'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_SR_UNSECURED'	, bancos['RTG_MDY_NSR_SR_UNSECURED'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_SUBORDINATED'	, bancos['RTG_MDY_NSR_SUBORDINATED'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_MDY_NSR_LT_CORP_FAMILY'	, bancos['RTG_MDY_NSR_LT_CORP_FAMILY'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_SP_NATIONAL'	, bancos['RTG_SP_NATIONAL'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_SP_NATIONAL_LT_ISSUER_CREDIT'	, bancos['RTG_SP_NATIONAL_LT_ISSUER_CREDIT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_NATIONAL_LT'	, bancos['RTG_FITCH_NATIONAL_LT'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_NATIONAL'	, bancos['RTG_FITCH_NATIONAL'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_NATIONAL_ST'	, bancos['RTG_FITCH_NATIONAL_ST'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_NATL_SUBORDINATED'	, bancos['RTG_FITCH_NATL_SUBORDINATED'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        contraparte_rtg.loc[i1]=[bancos['cnpj'][i].astype(str).zfill(14) , bancos['contraparte'][i]  , bancos['Nome Busca'][i], bancos['ISSUER'][i], 	'RTG_FITCH_NATIONAL_SR_UNSECURED'	, bancos['RTG_FITCH_NATIONAL_SR_UNSECURED'][i]	, dt_ref, data_bd]
        i1 = i1 + 1
        i = i + 1

    base_contraparte = contraparte_rtg[(~contraparte_rtg['rtg'].isnull())&(contraparte_rtg['rtg']!='#N/A Field Not Applicable')]

    #importar rating por isin
    isin = pd.read_excel(worksheet_path, sheetname='isin_sem_link', skiprows=1, header=0)

    isin_rtg = pd.DataFrame(columns=['cnpj', 'contraparte', 'isin',	'id_bloomberg', 'agencia_tipo_rtg', 'rtg', 'dt_ref', 'data_bd'])

    n = len(isin)
    i = 0
    i1= 0
    for i in range(0, n):
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_ST_DEBT_LC', isin['RTG_FITCH_ST_DEBT_LC'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_LT_ISSUER_DEFAULT', isin['RTG_FITCH_LT_ISSUER_DEFAULT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_OUTLOOK', isin['RTG_FITCH_OUTLOOK'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_ST_LC_DEBT', isin['RTG_MDY_ST_LC_DEBT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_LC_CURR_ISSUER_RATING', isin['RTG_MDY_LC_CURR_ISSUER_RATING'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_OUTLOOK', isin['RTG_MDY_OUTLOOK'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_SP', isin['RTG_SP'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_SP_ST_LC_ISSUER_CREDIT', isin['RTG_SP_ST_LC_ISSUER_CREDIT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MOODY', isin['RTG_MOODY'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MOODY_NO_WATCH', isin['RTG_MOODY_NO_WATCH'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MOODY_LONG_TERM', isin['RTG_MOODY_LONG_TERM'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MOODY_LONG_ISSUE_LEVEL', isin['RTG_MOODY_LONG_ISSUE_LEVEL'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MOODY_DES', isin['RTG_MOODY_DES'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_OUTLOOK', isin['RTG_MDY_OUTLOOK'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_SEN_UNSECURED_DEBT', isin['RTG_MDY_SEN_UNSECURED_DEBT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_INITIAL', isin['RTG_MDY_INITIAL'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_LC_CURR_ISSUER_RATING', isin['RTG_MDY_LC_CURR_ISSUER_RATING'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_LT_CORP_FAMILY', isin['RTG_MDY_LT_CORP_FAMILY'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_ISSUER', isin['RTG_MDY_NSR_ISSUER'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR', isin['RTG_MDY_NSR'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR', isin['RTG_MDY_NSR'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_ISSUER', isin['RTG_MDY_NSR_ISSUER'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_SHORT_TERM', isin['RTG_MDY_NSR_SHORT_TERM'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_SR_UNSECURED', isin['RTG_MDY_NSR_SR_UNSECURED'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_SUBORDINATED', isin['RTG_MDY_NSR_SUBORDINATED'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_MDY_NSR_LT_CORP_FAMILY', isin['RTG_MDY_NSR_LT_CORP_FAMILY'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_SP_NATIONAL', isin['RTG_SP_NATIONAL'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_SP_NATIONAL_LT_ISSUER_CREDIT', isin['RTG_SP_NATIONAL_LT_ISSUER_CREDIT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_NATIONAL_LT', isin['RTG_FITCH_NATIONAL_LT'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_NATIONAL', isin['RTG_FITCH_NATIONAL'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_NATIONAL_ST', isin['RTG_FITCH_NATIONAL_ST'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_NATL_SUBORDINATED', isin['RTG_FITCH_NATL_SUBORDINATED'][i], dt_ref, data_bd]
        i1 = i1 + 1
        isin_rtg.loc[i1]=[isin['cnpj'][i].astype(str).zfill(14), isin['contraparte'][i], isin['isin'][i] , isin['Nome Busca'][i], 'RTG_FITCH_NATIONAL_SR_UNSECURED', isin['RTG_FITCH_NATIONAL_SR_UNSECURED'][i], dt_ref, data_bd]
        i1 = i1 + 1
        i = i + 1

    isin_rtg_fim = isin_rtg[(~isin_rtg['rtg'].isnull())&(isin_rtg['rtg']!='#N/A Field Not Applicable')&(isin_rtg['rtg']!= '#N/A Invalid Security')]

    issuer = isin_rtg_fim[isin_rtg_fim['agencia_tipo_rtg'].str.contains('ISSUER')]
    isin_final = isin_rtg_fim[~isin_rtg_fim['agencia_tipo_rtg'].str.contains('ISSUER')]
    del isin_final['cnpj']
    del isin_final['contraparte']
    del issuer['isin']
    base_contraparte = base_contraparte.append(issuer)

    base_contraparte['cnpj'] = base_contraparte['cnpj'].str.split('.')
    base_contraparte['cnpj'] = base_contraparte['cnpj'].str[0]
    base_contraparte['rtg'] = base_contraparte['rtg'].replace(to_replace='#N/A Requesting Data...', value=None)

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True,
                            charset="utf8")

    pd.io.sql.to_sql(isin_final, name='rating_isin', con=connection, if_exists='append', flavor='mysql', index=0)
    pd.io.sql.to_sql(base_contraparte, name='rating_contraparte', con=connection, if_exists='append', flavor='mysql', index=0)

    connection.close()

    base_contraparte.to_excel(save_path + 'contrapartes.xlsx')

