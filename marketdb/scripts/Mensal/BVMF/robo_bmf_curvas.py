def robo_bmf_curvas(ano, mes, dia):

    lista_curvas=[
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
    
    import pandas as pd
    #Cria gabarito da tabela definitiva das curvas e também tabela aux2 que é pré-gabarito
    matriz_curvas=pd.DataFrame()
    aux2=pd.DataFrame()
    for i in range(0,5):
        matriz_curvas[i]=0
        aux2[i]=0
        
    matriz_curvas.columns=[
    'Prazo',
    '252',
    '360',
    'Valor',
    'Codigo']
        
    aux2.columns=[
    'Prazo',
    '252',
    '360',
    'Valor',
    'Codigo']
        
    
    
    # Acesso aos dados da página

    for curva in lista_curvas:
        #curva = lista_curvas[0]
        endereco_curvas="http://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?Data="+dia+"/"+mes+"/"+ano+"&slcTaxa="+curva
        dados_curvas=pd.read_html(endereco_curvas, thousands=".")
        
        if not(len(dados_curvas))<=2:#checa se essa curva tem dados, por meio do numero de linhas do dataframe, se <2, nao tem dados
            #Copia o dataframe para a tabela auxiliar
            aux=dados_curvas[2]
            
            #Na tabela auxiliar, verifica-se se possui os 2 campos '252', '360', ou apenas 1 campo
            #Faz-se uma copia das colunas de 'aux' para 'aux2', que é será adicionado (append) a matriz_curvas
            if (len(aux.columns))<3:
                if "252" in aux[0][1]:
                    aux=aux.drop([0,1])                #Elimina as 2 primeiras linhas (nao utilizaveis)
                    aux2['Prazo']=aux[0]
                    aux2['252']=aux[1]
                elif "360" in aux[0][1]:
                    aux=aux.drop([0,1])                 #Elimina as 2 primeiras linhas (nao utilizaveis)
                    aux2['Prazo']=aux[0]
                    aux2['360']=aux[1]
                else:
                    aux=aux.drop([0,1])                #Elimina as 2 primeiras linhas (nao utilizaveis)
                    aux2['Prazo']=aux[0]
                    aux2['Valor']=aux[1]
            
            else:
                aux=aux.iloc[2:len(aux),:]                    #Elimina as 2 primeiras linhas (nao utilizaveis)
                aux2['Prazo']=aux[0]
                aux2['252']=aux[1]
                aux2['360']=aux[2]
                
            aux2['Codigo']=curva
            matriz_curvas=matriz_curvas.append(aux2)
            for i in range (2,len(aux2)+2):
                aux2=aux2.drop(i)
            
    horario_bd=datetime.datetime.now()
    matriz_curvas["data_bd"]=horario_bd

    ano = int(ano)
    mes = int(mes)
    dia = int(dia)
    matriz_curvas["data_ref"]=datetime.date(ano,mes,dia)
        
    #troca , .
    matriz_curvas=matriz_curvas.replace({',':'.'}, regex=True)
    
    #troca NaN por None    
    matriz_curvas=matriz_curvas.where((pd.notnull(matriz_curvas)), None)
        
    import pymysql as db
    connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')
    
    pd.io.sql.to_sql(matriz_curvas, name='bmf_curvas', con=connection,if_exists="append", flavor='mysql', index=0)