def get_bmf_curvas_historico():
    import pandas as pd
    import pymysql as db
    import datetime
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    # Retorna um array (ano, mes e dia) referente ao útimo dia útil do mês anterior configurado no banco de dados
    dtbase = get_data_ultimo_dia_util_mes_anterior()

    # Retorna um range de datas do mes anterior
    dt_ref = pd.date_range(start=datetime.date(int(dtbase[0]),int(dtbase[1]),1),
                           end=datetime.date(int(dtbase[0]),int(dtbase[1]),int(dtbase[2])),
                           freq='D').date

    # Para cada dia de referência do mês anterior, puxa as informações do site da bolsa e inseri no banco
    for i in dt_ref:
        print("Executando para o dia: " + str(i))
        dia = str(i.day)
        mes = str(i.month)
        ano = str(i.year)

        if len(mes) == 1:
            mes = "0" + mes

        if len(dia) == 1:
            dia = "0" + dia

        lista_curvas = [
            "ACC",
            "ALD",
            "AN",
            "ANP",
            "APR",
            "BRP",
            "CBD",
            "DCO",
            "DIC",
            "DIM",
            "DOC",
            "DOL",
            "DP",
            "EUC",
            "EUR",
            "IAP",
            "INP",
            "IPR",
            "JPY",
            "LIB",
            "NID",
            "PBD",
            "PDN",
            "PRE",
            "PTX",
            "SDE",
            "SLP",
            "SND",
            "TFP",
            "TP",
            "TR",
            "ZND"
        ]

        # Cria gabarito da tabela definitiva das curvas e também tabela aux2 que é pré-gabarito
        matriz_curvas = pd.DataFrame()
        aux2 = pd.DataFrame()
        for j in range(0, 5):
            matriz_curvas[j] = 0
            aux2[j] = 0

        matriz_curvas.columns = [
            'Prazo',
            '252',
            '360',
            'Valor',
            'Codigo']

        aux2.columns = [
            'Prazo',
            '252',
            '360',
            'Valor',
            'Codigo']

        # Acesso aos dados da página
        try:
            for curva in lista_curvas:
                # curva = lista_curvas[0]
                endereco_curvas = "http://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?Data=" + dia + "/" + mes + "/" + ano + "&slcTaxa=" + curva
                dados_curvas = pd.read_html(endereco_curvas, thousands=".")

                if not (len(
                        dados_curvas)) <= 2:  # checa se essa curva tem dados, por meio do numero de linhas do dataframe, se <2, nao tem dados
                    # Copia o dataframe para a tabela auxiliar
                    aux = dados_curvas[2]

                    # Na tabela auxiliar, verifica-se se possui os 2 campos '252', '360', ou apenas 1 campo
                    # Faz-se uma copia das colunas de 'aux' para 'aux2', que é será adicionado (append) a matriz_curvas
                    if (len(aux.columns)) < 3:
                        if "252" in aux[0][1]:
                            aux = aux.drop([0, 1])  # Elimina as 2 primeiras linhas (nao utilizaveis)
                            aux2['Prazo'] = aux[0]
                            aux2['252'] = aux[1]
                        elif "360" in aux[0][1]:
                            aux = aux.drop([0, 1])  # Elimina as 2 primeiras linhas (nao utilizaveis)
                            aux2['Prazo'] = aux[0]
                            aux2['360'] = aux[1]
                        else:
                            aux = aux.drop([0, 1])  # Elimina as 2 primeiras linhas (nao utilizaveis)
                            aux2['Prazo'] = aux[0]
                            aux2['Valor'] = aux[1]

                    else:
                        aux = aux.iloc[2:len(aux), :]  # Elimina as 2 primeiras linhas (nao utilizaveis)
                        aux2['Prazo'] = aux[0]
                        aux2['252'] = aux[1]
                        aux2['360'] = aux[2]

                    aux2['Codigo'] = curva
                    matriz_curvas = matriz_curvas.append(aux2)
                    for i in range(2, len(aux2) + 2):
                        aux2 = aux2.drop(i)
        except:
            pass

        horario_bd = datetime.datetime.now()
        matriz_curvas["data_bd"] = horario_bd

        ano = int(ano)
        mes = int(mes)
        dia = int(dia)
        matriz_curvas["data_ref"] = datetime.date(ano, mes, dia)

        # troca , .
        matriz_curvas = matriz_curvas.replace({',': '.'}, regex=True)

        # troca NaN por None
        matriz_curvas = matriz_curvas.where((pd.notnull(matriz_curvas)), None)

        connection = db.connect('localhost', user='root', passwd="root", db='projeto_inv')

        pd.io.sql.to_sql(matriz_curvas, name='bmf_curvas', con=connection, if_exists="append", flavor='mysql', index=0)

        connection.close()
