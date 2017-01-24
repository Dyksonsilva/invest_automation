def get_bacen_series_hist():
    import pymysql as db
    import pandas as pd
    import numpy as np
    import datetime
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from os import listdir

    # Retorna o path utilizado para acesso aos dados baixados
    base_dir = full_path_from_database("bacen")

    lista_arquivos = listdir(base_dir)

    output = pd.DataFrame(columns=['data_referencia','valor','codigo','nome','frequencia','data_bd'])

    # Apenda os arquivos em uma estrutura utilizada a posteriori
    for i in lista_arquivos:

        connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True,
                                charset="utf8")

        aux = pd.DataFrame(columns=['data_referencia', 'valor', 'codigo', 'nome', 'frequencia', 'data_bd'])
        tabela = pd.read_csv(base_dir+i, skiprows=0, sep =";",header=0, encoding ="iso-8859-1")

        #Tira a última linha que é coisa do bacen
        tabela = tabela[0:len(tabela)-1]

        #Pega o número da série pelo header das colunas
        codigo = tabela.columns.values
        codigo = codigo[1]
        codigo = codigo.split('-')
        codigo = codigo[0].replace(' ','')

        #Puxa a informação da bacen_series da bd
        query = 'select * from projeto_inv.bacen_series where codigo = ' + codigo
        bacen_series = pd.read_sql(query, con=connection)
        nome = bacen_series['nome'].iloc[0]
        frequencia = bacen_series['frequencia'].iloc[0]

        tabela.columns =['dt_ref','taxa']

        #Data de referencia
        tabela['dt_ref'] = tabela['dt_ref'].str.split('/')
        tabela['tamanho'] = tabela['dt_ref'].str.len()
        if tabela['tamanho'].iloc[0]==2:
            tabela['dt_ref'] = tabela['dt_ref'].str[1] + tabela['dt_ref'].str[0] + '01'
        else:
            tabela['dt_ref'] = tabela['dt_ref'].str[2] + tabela['dt_ref'].str[1] + tabela['dt_ref'].str[0]

        aux['data_referencia'] = tabela['dt_ref']
        aux['valor'] = tabela['taxa']
        aux['codigo'] = codigo
        aux['nome'] = nome
        aux['frequencia'] = frequencia

        output = output.append(aux)

    #Finalização
    output['data_bd'] = datetime.datetime.today()
    output['codigo'] = output['codigo'].astype(int)
    output['data_referencia'] = pd.to_datetime(output['data_referencia']).dt.date
    output['valor'] = output['valor'].str.replace(',','.')
    output['valor'] = output['valor'].astype(float)
    output['data_bd'] = datetime.datetime.today()

    output['nome'] = np.where(output['codigo']==7811,'TR',output['nome'])

    pd.io.sql.to_sql(output, name='bacen_series', con=connection, if_exists="append", flavor='mysql', index=False)

    #Preenchimento do bacen_series_hist

    output = output[output.codigo.isin([7811,189,256,433,4389])].copy()

    bacen_series_hist = pd.DataFrame(columns=['valor','indice','ano','mes','dia','dt_ref'])

    bacen_series_hist['valor'] = output['valor']
    bacen_series_hist['indice'] = output['codigo'].astype(str)
    bacen_series_hist['dt_ref'] = output['data_referencia']


    bacen_series_hist['indice'] = bacen_series_hist['indice'].str.replace('7811','TR')
    bacen_series_hist['indice'] = bacen_series_hist['indice'].str.replace('189','IGP')
    bacen_series_hist['indice'] = bacen_series_hist['indice'].str.replace('256','TJLP')
    bacen_series_hist['indice'] = bacen_series_hist['indice'].str.replace('433','IPCA')
    bacen_series_hist['indice'] = bacen_series_hist['indice'].str.replace('4389','DI1')

    bacen_series_hist['dt_ref1'] = bacen_series_hist['dt_ref'].astype(str)
    bacen_series_hist['dt_ref1'] = bacen_series_hist['dt_ref1'].str.split('-')
    bacen_series_hist['ano'] = bacen_series_hist['dt_ref1'].str[0]
    bacen_series_hist['mes'] = bacen_series_hist['dt_ref1'].str[1]
    bacen_series_hist['dia'] = bacen_series_hist['dt_ref1'].str[2]

    del bacen_series_hist['dt_ref1']

    #Salvar no MySQL
    pd.io.sql.to_sql(bacen_series_hist, name='bacen_series_hist', con=connection, if_exists='append', flavor='mysql', index=0)

    connection.close()