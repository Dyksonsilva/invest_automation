def finalizacao_fidc():

    import datetime
    import pandas as pd
    import numpy as np
    import pymysql as db
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]
    dt_base_rel = datetime.date(int(dtbase[0]), int(dtbase[1]), int(dtbase[2]))

    # Diretório de save de planilhas
    save_path_puposicao_final = full_path_from_database('get_output_quadro419') + 'puposicao_final_deb.xlsx'

    horario_inicio= datetime.datetime.now()
    tol = 0.20
    horario_bd = datetime.datetime.today()

    ###############################################################################
    #1 - Leitura e criação de tabelas
    ###############################################################################

    #Informações do cálculo de MTM
    connection = db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv')

    query = "SELECT * FROM projeto_inv.mtm_titprivado WHERE tipo_ativo = 'CTF'"

    mtm_titprivado0 = pd.read_sql(query, con=connection)

    connection.close()

    mtm_titprivado = mtm_titprivado0.copy()

    #Seleciona debentures
    #mtm_titprivado = mtm_titprivado[mtm_titprivado.tipo_ativo.isin(['CTF'])].copy()

    #Seleciona a última carga de debentures da data da posicao
    mtm_titprivado['dtrel'] = mtm_titprivado['id_papel'].str.split('_')
    mtm_titprivado['dtrel'] = mtm_titprivado['dtrel'].str[0]

    mtm_titprivado = mtm_titprivado[mtm_titprivado.dtrel == dtbase_concat].copy()
    mtm_titprivado = mtm_titprivado[mtm_titprivado.data_bd == max(mtm_titprivado.data_bd)]

    mtm_titprivado = mtm_titprivado.rename(columns={'data_fim':'dt_ref'})

    #Reajusta papéis indesaxos a DI
    mtm_titprivado['dt_ref'] = pd.to_datetime(mtm_titprivado['dt_ref'])
    mtm_titprivado['dt_ref'] = np.where(mtm_titprivado['indexador']=='DI1', mtm_titprivado['dt_ref'] + pd.DateOffset(months=0, days=1), mtm_titprivado['dt_ref'])
    mtm_titprivado['dt_ref'] = mtm_titprivado['dt_ref'].dt.date

    #Segura apenas fluxos positivos
    mtm_titprivado = mtm_titprivado[mtm_titprivado.prazo_du>=0].copy()

    del mtm_titprivado['data_bd']
    del mtm_titprivado['dtrel']

    ###############################################################################
    #2 - Calculo de DV100 e Duration
    ###############################################################################

    mtm_titprivado['DV100'] = (mtm_titprivado['mtm'] - mtm_titprivado['mtm_DV100'])/(mtm_titprivado['mtm'])

    #Duration
    mtm_titprivado['duration'] = mtm_titprivado['perc_mtm']*mtm_titprivado['prazo_du']

    ###############################################################################
    #3 - Finalização e upload na base
    ###############################################################################

    mtm_titprivado['mtm'] = None

    mtm_titprivado['data_bd'] = horario_bd

    mtm_titprivado['ano_mtm'] = mtm_titprivado['ano_mtm'].astype(str)
    mtm_titprivado['mes_mtm'] = mtm_titprivado['mes_mtm'].astype(str)
    mtm_titprivado['mes_mtm'] = mtm_titprivado['mes_mtm'].str.zfill(2)
    mtm_titprivado['dia_mtm'] = mtm_titprivado['dia_mtm'].astype(str)
    mtm_titprivado['dia_mtm'] = mtm_titprivado['dia_mtm'].str.zfill(2)

    mtm_titprivado['data_mtm'] = mtm_titprivado['ano_mtm'].astype(str) + mtm_titprivado['mes_mtm'].astype(str) + mtm_titprivado['dia_mtm'].astype(str)
    mtm_titprivado['data_mtm'] = pd.to_datetime(mtm_titprivado['data_mtm']).dt.date

    #Tabelas não necessárias - MTM
    del mtm_titprivado['indice_du_mtm']
    del mtm_titprivado['indice_dc_mtm']
    del mtm_titprivado['ano_dt_ref2']
    del mtm_titprivado['mes_dt_ref2']
    del mtm_titprivado['dia_dt_ref2']
    del mtm_titprivado['vertices_positivo']
    del mtm_titprivado['indice_dc_dt_ref2']
    del mtm_titprivado['indice_du_dt_ref2']
    del mtm_titprivado['prazo_dc']
    del mtm_titprivado['ano_inicio']
    del mtm_titprivado['mes_inicio']
    del mtm_titprivado['dia_inicio']
    del mtm_titprivado['indice_du_inicio']
    del mtm_titprivado['indice_dc_inicio']
    del mtm_titprivado['ano_fim']
    del mtm_titprivado['mes_fim']
    del mtm_titprivado['dia_fim']
    del mtm_titprivado['indice_du_fim']
    del mtm_titprivado['indice_dc_fim']
    del mtm_titprivado['dt_ref']
    del mtm_titprivado['dtoperacao']

    connection = db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv')
    pd.io.sql.to_sql(mtm_titprivado, name='mtm_renda_fixa', con=connection, if_exists='append', flavor='mysql', index=0)
    connection.close()
