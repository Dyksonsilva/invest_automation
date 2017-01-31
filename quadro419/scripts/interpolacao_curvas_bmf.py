def interpolacao_curvas_bmf():

    import pymysql as db
    import pandas as pd
    import datetime
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    #dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    #Conexão com Banco de Dados
    connection = db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv')

    def interpolacao(tp_curva, dt_ref):

        #dt_ref = '2016-12-19'
        #tp_curva = 'PRE'

        global curva_ettj
        global curva_ettj_fim
        global list_of_values
        global amostra

        try:
            ano = str(dt_ref[0:4])
            mes = str(dt_ref[5:7])
            dia = str(dt_ref[8:10])
            data = datetime.date(int(ano), int(mes), int(dia))

            query = 'select * from projeto_inv.bmf_curvas_' + str(ano) + '_' + str(mes) + ' where data_ref='+'"'+dt_ref+'" and codigo='+'"'+tp_curva+'";'
            #query = "select * from projeto_inv.bmf_curvas where data_ref='2016-12-07' and codigo='PRE'"
            amostra = pd.read_sql(query, con=connection)
            amostra = amostra.rename(columns={'id_bmf_curva_' + str(ano) + '_' + str(mes): 'id_bmf_curva'})

            if tp_curva=='DP':
                ref_data=360
                ref_data_txt='360'
            else:
                ref_data=252
                ref_data_txt='252'
            #listar as colunas da tabela:  tp_mtm.columns.tolist()


            #amostra=curvas[curvas['Codigo']==tp_curva]
            amostra=amostra.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
            amostra.index = amostra.index + 1
            #prazo_max=max(amostra['Prazo'])

            if tp_curva=='DP':
                prazo_max=2520
                list_of_values = [21, 63, 126, 252, 378, 504, 630 , 756, 1008, 1260, 2520]

            elif tp_curva=='DIM':
                prazo_max=12600
                list_of_values = [63, 126, 252, 378, 504, 630, 756, 1008, 1260, 2520, 3780, 5040, 6300, 7560, 8820, 10080, 11340, 12600]

            elif tp_curva=='DIC':
                prazo_max=12600
                list_of_values = [63, 126, 252, 378, 504, 630, 756, 1008, 1260, 2520, 3780, 5040, 6300, 7560, 8820, 10080, 11340, 12600]

            elif tp_curva=='TP':
                prazo_max=12600
                list_of_values = [63, 126, 252, 378, 504, 630, 756, 1008, 1260, 2520, 3780, 5040, 6300, 7560, 8820, 10080, 11340, 12600]

            else:
                prazo_max=3780
                list_of_values=[21, 63, 126, 252, 378, 504, 630, 756, 1008, 1260, 2520, 3780]


            #curva_ettj=pd.DataFrame(columns=['id_bmf_curva', 'prazo','tx_spot', 'tx_spot_ano'])
            curva_ettj=pd.DataFrame(columns=['id_bmf_curva', 'prazo','tx_spot'])
            curva_ettj1=pd.DataFrame(columns=['prazo','tx_spot'])

            max_vertice=max(amostra['Prazo'])
            i=1
            i1=1
            for i1 in range(1,min(prazo_max,max_vertice)+1):
            #for i1 in range(1, prazo_max):
                if i1==amostra['Prazo'][i]:
                    curva_ettj.loc[i1]=[amostra['id_bmf_curva'][i], i1, (1+amostra[ref_data_txt][i]/100)**(i1/ref_data)-1]
                    #curva_ettj['tx_spot_ano']=(1+curva_ettj['tx_spot'])**(ref_data/curva_ettj['prazo'])-1
                    i1=i1+1
                    i=i+1

                elif i1<amostra['Prazo'][i]:
                    curva_ettj.loc[i1]=[amostra['id_bmf_curva'][i], i1, (1+amostra[ref_data_txt][i]/100)**(i1/ref_data)-1]
                    #curva_ettj['tx_spot_ano']=(1+curva_ettj['tx_spot'])**(ref_data/curva_ettj['prazo'])-1
                    i1=i1+1

                #elif i1>max_vertice:

                else:
                    #curva_ettj['tx_spot_ano'][i1]=curva_ettj['tx_spot_ano'][i1-1]
                    #curva_ettj['tx_spot'][i]=((1+curva_ettj['tx_spot_ano'][i1])**(curva_ettj['prazo'][i1]/ref_data))-1
                    curva_ettj.loc[i1]=[amostra['id_bmf_curva'][i], i1, (1+curva_ettj['tx_spot'][i1-1])*(((1+amostra[ref_data_txt][i+1]/100)**(amostra['Prazo'][i+1]/ref_data))/(1+curva_ettj['tx_spot'][i1-1]))**((i1-curva_ettj['prazo'][i1-1])/(amostra['Prazo'][i+1]-curva_ettj['prazo'][i1-1]))-1]
                    i1=i1+1

            while i1<(prazo_max+1):
                    #curva_ettj1['prazo'][0]=curva_ettj['prazo'][i1-1]+1
                    #curva_ettj1['tx_spot'][1]= (1+curva_ettj['tx_spot'][max_vertice])**(curva_ettj['prazo'][i1]/curva_ettj['prazo'][max_vertice])
                curva_ettj1.loc[i1]= [i1, ((1+curva_ettj['tx_spot'][max_vertice])**(ref_data/curva_ettj['prazo'][max_vertice]))**(i1/ref_data)-1]
                i1=i1+1

            curva_ettj=curva_ettj.append(curva_ettj1)
            curva_ettj['prazo']= curva_ettj['prazo'].astype(int)
            #curva_ettj['id_bmf_curva']= curva_ettj['id_bmf_curva'].astype(int)
            curva_ettj['tx_spot_ano']=(1+curva_ettj['tx_spot'])**(ref_data/curva_ettj['prazo'])-1
            curva_ettj['tx_spot_shift']=curva_ettj['tx_spot'].shift()
            curva_ettj['tx_spot_shift']=curva_ettj['tx_spot_shift'].fillna(0)
            curva_ettj['tx_termo_dia']=(1+curva_ettj['tx_spot'])/(1+curva_ettj['tx_spot_shift'])-1
            curva_ettj['indexador_cod']=tp_curva
            curva_ettj['dt_ref']=dt_ref

            del curva_ettj['tx_spot_shift']
            curva_ettj['prazo']=curva_ettj['prazo'].astype(int)
            curva_ettj['data_bd'] = datetime.datetime.now()
            curva_ettj_fim=curva_ettj[curva_ettj['prazo'].isin(list_of_values)]

            #Salvar no MySQL
            print("inserting into curva_ettj_vertices_fixos.......... ")
            pd.io.sql.to_sql(curva_ettj_fim, name='curva_ettj_vertices_fixos', con=connection, if_exists='append', flavor='mysql', index=0)
            print("inserting into curva_ettj_interpol_ano_mes.......... ")
            pd.io.sql.to_sql(curva_ettj, name='curva_ettj_interpol_' + str(ano) + '_' + str(mes), con=connection, if_exists='append', flavor='mysql', index=0)

        except:
            print('Deu ruim...')
            return None

        # se for rodar a base diária

    dt_ref = pd.date_range (start=datetime.date(int(dtbase[0]),int(dtbase[1]),1), end=datetime.date(int(dtbase[0]),int(dtbase[1]),int(dtbase[2])), freq='D').date
    for dt in dt_ref:

        dt = str(dt)

        try:
            print("Fazendo interpolacao para PRE, data --> " + str(dt))
            interpolacao('PRE', dt)
            print("Finalizado PRE")
            print("Fazendo interpolacao para DIC, data --> " + str(dt))
            interpolacao('DIC', dt)
            print("Finalizado DIC")
            print("Fazendo interpolacao para DIM, data --> " + str(dt))
            interpolacao('DIM', dt)
            print("Finalizado DIM")
            interpolacao('DP', dt)
            print("Fazendo interpolacao para DP, data --> " + str(dt))
            print("Finalizado DP")
            interpolacao('TP', dt)
            print("Finalizado TP")

            print(dt, 'feito')

        except:

            print(dt)

    connection.close()
