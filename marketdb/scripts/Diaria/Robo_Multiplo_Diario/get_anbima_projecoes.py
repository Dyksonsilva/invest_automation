def robos_anbima_projecoes():
    import pandas as pd
    import datetime
    
    
    #Diretório para salvar xlsx
    #save_path = 'C:/Users/Cora.Santos/Desktop/HDI-Investimentos/Robos'
    
    #Páginas com dado mais atual
#    endereco_ipca ="http://portal.anbima.com.br/informacoes-tecnicas/precos/indices-de-precos/Pages/ipca.aspx"
#    endereco_igpm ="http://portal.anbima.com.br/informacoes-tecnicas/precos/indices-de-precos/Pages/igp-m.aspx"
    
    endereco_ipca ="http://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"
    endereco_igpm ="http://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm"
    
    pagina_ipca = pd.read_html(endereco_ipca, thousands =".", encoding="utf8")
    pagina_igpm = pd.read_html(endereco_igpm, thousands = ".", encoding="utf8")
    
    #Separação informações mês corrente e próximo mês
    ipca_mes_corrente = pagina_ipca[0]
    igpm_mes_corrente = pagina_igpm[0]
    ipca_mes_posterior= pagina_ipca[1]
    igpm_mes_posterior = pagina_igpm[1]
    
    #Colocar nome do índice nos dataframes
    ipca_mes_corrente["indice"] = "IPCA"
    ipca_mes_posterior["indice"] = "IPCA"
    igpm_mes_corrente["indice"] = "IGPM"
    igpm_mes_posterior["indice"] = "IGPM"
    
    #Colocar mês da previsão
    ipca_mes_corrente["mes_previsao"] = "Corrente"
    ipca_mes_posterior["mes_previsao"] = "Posterior"
    igpm_mes_corrente["mes_previsao"] = "Corrente"
    igpm_mes_posterior["mes_previsao"] = "Posterior"
    
    #Colocar nomes nas colunas
    ipca_mes_corrente.columns =["mes_coleta", "data_coleta", "projecao", "data_validade", "indice" , "mes_previsao"]
    igpm_mes_corrente.columns =["mes_coleta", "data_coleta", "projecao", "data_validade", "indice" , "mes_previsao"]
    
    ipca_mes_posterior.columns =["mes_coleta", "data_coleta", "projecao", "indice" , "mes_previsao"]
    igpm_mes_posterior.columns =["mes_coleta", "data_coleta", "projecao", "indice" , "mes_previsao"]
    
    
    #Eliminar duas primeiras linhas dos dataframes
    ipca_mes_corrente = ipca_mes_corrente.ix[2:]
    igpm_mes_corrente = igpm_mes_corrente.ix[2:]
    ipca_mes_posterior= ipca_mes_posterior.ix[2:]
    igpm_mes_posterior = igpm_mes_posterior.ix[2:]
    
    
    #Concatenar valores
    df_final = ipca_mes_corrente
    df_final = df_final.append(igpm_mes_corrente)
    df_final = df_final.append(ipca_mes_posterior)
    df_final = df_final.append(igpm_mes_posterior)
    
    #Eliminar linhas sem valores
    df_final = df_final[df_final["projecao"]!="-"]
    
    #Elimina caractere '\u200b' que gera problema de formatação
    df_final=df_final.replace({'\u200b': ''}, regex=True)    
    
    ## Tranformar data_validade e data_coleta em formato de data válido
    df_final["data_validade"] =pd.to_datetime(df_final["data_validade"], format='%d/%m/%Y')
    df_final["data_coleta"] =pd.to_datetime(df_final["data_coleta"], format='%d/%m/%Y')
    
    
    #Retirar horas e minutos da conversão feita no passo anterior
    df_final["data_validade"] = df_final["data_validade"].dt.date
    df_final["data_coleta"] = df_final["data_coleta"].dt.date
    
    # Trocar vírgula por ponto
    df_final = df_final.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
    df_final["projecao"][len(df_final)-1] = df_final["projecao"][len(df_final)-1].strip()#.str.replace(' ','')
    df_final["projecao"] = df_final["projecao"].str.replace(',','.')
    df_final["projecao"] = df_final["projecao"][0:4]
    df_final["projecao"] = df_final["projecao"].astype(float)
    
    #[float(i.replace(",",".")) for i in df_final["projecao"]]
    
    #Colocar data_bd
    df_final["data_bd"] = datetime.datetime.now()
    
    #df_final.to_excel(save_path+'/projecoes_anbima.xlsx')
    
    #Salvar informação no BD
    import pymysql as db
    connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv',use_unicode=True, charset="utf8")
    pd.io.sql.to_sql(df_final, name='anbima_projecao_indices', con=connection,if_exists="append", flavor='mysql', index=0)
    