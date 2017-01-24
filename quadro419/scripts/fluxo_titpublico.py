def fluxo_titpublico():
    import datetime
    import pandas as pd
    import numpy as np
    import pymysql as db
    from pandas.tseries.offsets import DateOffset
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    #Conexão com Banco de Dados
    connection = db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv')

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()

    dtbase_datetime = datetime.date(int(dtbase[0]),int(dtbase[1]),int(dtbase[2]))

    query = 'SELECT * FROM projeto_inv.anbima_tpf'

    baseref1 = pd.read_sql(query, con=connection)
    #1 - ALTERACAO DO CODIGO POR MUDANCA DA QUERY
    #1.1 - Joga fora sujeira da base antiga
    baseref1 = baseref1[baseref1['dt_vencto2'].notnull()]
    #1.2 - Ordena pela data da carga
    baseref1 = baseref1.sort_values(by = ['dt_referencia', 'titulo', 'cod_selic','dt_emissao','dt_vencto','dt_carga'], ascending = True)
    #1.3 - Pega para cada um dos titulos publicos apenas a ultima informacao baixada da anbima
    baseref1 = baseref1.drop_duplicates(subset = ['titulo', 'cod_selic','dt_emissao','dt_vencto'], keep = 'last')
    #1.4 - Joga fora titulos que ja venceram
    baseref1 = baseref1[baseref1['dt_vencto2'] >= dtbase_datetime]
    #1.5 - Reseta o index do dataframe
    baseref1 = baseref1.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    ###############################################################################

    anbima_fluxo_tpf = pd.DataFrame(columns=['titulo',
                                             'cod_selic',
                                             'dt_emissao',
                                             'dt_vencto',
                                             'dt_vencto2',
                                             'dt_referencia',
                                             'dt_carga',
                                             'dt_ref',
                                             'fv'])

    n = len(baseref1)

    i = 0
    i1 = 0

    for i in range(0 , n):
        qtde = 1
        vencto = pd.to_datetime(baseref1['dt_vencto2'][i])
        temp = pd.to_datetime(baseref1['dt_vencto2'][i])
        anbima_fluxo_tpf.loc[i1] = [baseref1['titulo'][i], baseref1['cod_selic'][i], baseref1['dt_emissao'][i], baseref1['dt_vencto'][i], baseref1['dt_vencto2'][i], baseref1['dt_referencia'][i], baseref1['dt_carga'][i], temp, 0]
        if (baseref1['titulo'][i] in ('NTN-B' , 'NTN-C', 'NTN-F')):
            #print(anbima_fluxo_tpf)
            temp = temp - DateOffset(months=6, days=0)
            while temp > pd.to_datetime(baseref1['dt_emissao'])[i]:
                i1 = i1 + 1
                qtde = qtde + 1
                anbima_fluxo_tpf.loc[i1] = [baseref1['titulo'][i], baseref1['cod_selic'][i], baseref1['dt_emissao'][i], baseref1['dt_vencto'][i], baseref1['dt_vencto2'][i], baseref1['dt_referencia'][i], baseref1['dt_carga'][i], temp, 0]
                temp = vencto - DateOffset(months=(6*qtde), days=0)
        i = i + 1
        i1 = i1 + 1

    del anbima_fluxo_tpf['dt_referencia']
    del anbima_fluxo_tpf['dt_carga']

    anbima_fluxo_tpf['dt_ref'] = pd.to_datetime(anbima_fluxo_tpf['dt_ref'], format='%d-%m-%Y')

    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'LFT', 'cupom'] = 0
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'LTN', 'cupom'] = 0
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'NTN-C', 'cupom'] = 0.06
    anbima_fluxo_tpf.ix[(anbima_fluxo_tpf['titulo']=='NTN-C') & (pd.to_datetime(anbima_fluxo_tpf['dt_vencto']).dt.date==datetime.date(2031, 1,1)), 'cupom'] = 0.12
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'NTN-B', 'cupom'] = 0.06
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'NTN-F', 'cupom'] = 0.10
    anbima_fluxo_tpf['dif']=(pd.to_datetime(anbima_fluxo_tpf['dt_vencto2'])-pd.to_datetime(anbima_fluxo_tpf['dt_ref'])).astype('timedelta64[D]')*1
    anbima_fluxo_tpf['fv'] = np.where(anbima_fluxo_tpf.dif <= 10.0, (1+(anbima_fluxo_tpf['cupom']))**0.5,(1+(anbima_fluxo_tpf['cupom']))**0.5-1)
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'LTN', 'fv']=1
    anbima_fluxo_tpf.ix[anbima_fluxo_tpf.titulo == 'LFT', 'fv']=1
    del anbima_fluxo_tpf['dif']

    anbima_fluxo_tpf['data_bd'] = pd.datetime.today()

    anbima_fluxo_tpf1 = anbima_fluxo_tpf.copy()

    anbima_fluxo_tpf1['dt_ref'] = anbima_fluxo_tpf1['dt_ref'].dt.date

    # Salvar no MySQL
    if len(anbima_fluxo_tpf1) > 0:
        pd.io.sql.to_sql(anbima_fluxo_tpf1, name='anbima_fluxo_tpf', con=connection, if_exists='append', flavor='mysql', index=0)

    connection.close()