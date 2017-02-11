def quaid419(id_relatorio_qo, dtbase, ent_codigo, tipo_relatorio, horario_processo):

    import pandas as pd
    import numpy as np
    import pymysql as db
    import logging
    import datetime
    from .atvcodigo import atvcodigo
    from .lcrcodigo import lcrcodigo

    logger = logging.getLogger(__name__)

    ## Gerar arquivo base
    colunas= ["EMFSEQ",
              "ENTCODIGO",
              "MRFMESANO",
              "QUAID",
              "ATVCODIGO",
              "TPFOPERADOR",
              "FTRCODIGO",
              "LCRCODIGO",
              "TCTCODIGO",
              "TPECODIGO",
              "EMFPRAZOFLUXO",
              "EMFVLREXPRISCO",
              "EMFCNPJFUNDO",
              "EMFCODISIN",
              "EMFCODCUSTODIA",
              "EMFMULTIPLOFATOR",
              "EMFTXCONTRATADO",
              "EMFTXMERCADO",
              "TPFOPERADORDERIVATIVO",
              "EMFVLRDERIVATIVO",
              "EMFCODGRUPO",
              "tipo_produto"]
    quaid_419 = pd.DataFrame(columns=colunas)

    futuros_ajustados = pd.DataFrame()

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")

    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * from projeto_inv.xml_quadro_operacoes where id_relatorio_qo='+str(id_relatorio_qo)
    arquivo_operacoes = pd.read_sql_query(query, connection)

    logger.info("Leitura do banco de dados executada com sucesso")

    logger.info("Tratando dados")

    #Altera o indexador dos títulos públicos que possam vir errados
    arquivo_operacoes['indexador'] = np.where((arquivo_operacoes['produto'].isin(['titulo público']))&(arquivo_operacoes['ativo'].isin(['760100','760199'])),'IAP',arquivo_operacoes['indexador'])
    arquivo_operacoes['indexador'] = np.where((arquivo_operacoes['produto'].isin(['titulo público']))&(arquivo_operacoes['ativo'].isin(['770100'])),'IGM',arquivo_operacoes['indexador'])
    arquivo_operacoes['indexador'] = np.where((arquivo_operacoes['produto'].isin(['titulo público']))&(arquivo_operacoes['ativo'].isin(['210100'])),'SEL',arquivo_operacoes['indexador'])

    query_fluxos = 'select a.codigo_isin, a.perc_mtm, a.tipo_ativo, a.prazo_du, a.indexador, a.taxa_juros, a.data_mtm, a.data_bd from projeto_inv.mtm_renda_fixa a right join (select codigo_isin, data_mtm, max(data_bd) as data_bd from projeto_inv.mtm_renda_fixa where data_mtm="'+dtbase+'" group by 1,2) b on a.codigo_isin=b.codigo_isin and a.data_mtm=b.data_mtm and a.data_bd=b.data_bd;'
    fluxos = pd.read_sql_query(query_fluxos, connection)
    logger.info("Leitura do banco de dados executada com sucesso")
    
    #Separação da taxa_operação vindo de cadastro
    indexadores = fluxos[['codigo_isin','taxa_juros','tipo_ativo']].copy()
    indexadores = indexadores.drop_duplicates(subset=['codigo_isin'])
    indexadores = indexadores[indexadores['tipo_ativo']!='TPF'].copy()
    
    arquivo_operacoes = pd.merge(arquivo_operacoes,indexadores,left_on=['isin'],right_on=['codigo_isin'],how='left')
    arquivo_operacoes['tx_operacao'] = np.where(arquivo_operacoes['tipo_ativo'].notnull(),arquivo_operacoes['taxa_juros'],arquivo_operacoes['tx_operacao'])
    
    del arquivo_operacoes['codigo_isin']
    del arquivo_operacoes['taxa_juros']
    del arquivo_operacoes['tipo_ativo']

    ### Código para dar o FTRCODIGO
    def ftrcodigo(numero_linha):
        if arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and arquivo_operacoes["tx_operacao"].iloc[numero_linha]==0:
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and arquivo_operacoes["tx_operacao"].iloc[numero_linha]!=0:
            return "TD2"
        elif "undo" in arquivo_operacoes["produto"].iloc[numero_linha] :
            return "FF1"
        elif "DI1" in str(arquivo_operacoes["ativo"].iloc[numero_linha]) and "uturo" in arquivo_operacoes["produto"].iloc[numero_linha]:
            return "JJ1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "DI1" and np.isnan(arquivo_operacoes["tx_operacao"].iloc[numero_linha]):
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IAP":
            return "JI1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "ANB":
            return "TD1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IPC":
            return "JI1"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "IGM":
            return "JI2"
        elif arquivo_operacoes["indexador"].iloc[numero_linha] == "SEL":
            return "TS1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "ações":
            return "AA1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "uturo" and "IND" in arquivo_operacoes["isin"].iloc[numero_linha] :
            return "AA1"
        elif arquivo_operacoes["produto"].iloc[numero_linha] == "uturo" and "DOL" in arquivo_operacoes["isin"].iloc[numero_linha] :
            return "ME1"
        else:
            return "JJ1"

    for linha in range(len(arquivo_operacoes)):
        posicao ={}
        posicao["EMFSEQ"] = linha+1
        posicao["ENTCODIGO"] = ent_codigo
        posicao["MRFMESANO"] = dtbase
        posicao["QUAID"] = 419
        posicao["ATVCODIGO"]= atvcodigo(arquivo_operacoes["produto"].iloc[linha])

        if arquivo_operacoes["a_p_op"].iloc[linha] == 'V':
            posicao["TPFOPERADOR"] = '-'
        else:
            posicao["TPFOPERADOR"] = '+'

        posicao["FTRCODIGO"]= ftrcodigo(linha)
        posicao["LCRCODIGO"]= lcrcodigo(arquivo_operacoes["produto"].iloc[linha])
        posicao["EMFCNPJFUNDO"]= arquivo_operacoes["cnpjfundo_1nivel"].iloc[linha]
        posicao["TCTCODIGO"]= "02" if posicao["EMFCNPJFUNDO"]==None else "01"

        if "blico" in (arquivo_operacoes["produto"].iloc[linha]):
            posicao["TPECODIGO"] = 'PU01'
        else:
            posicao["TPECODIGO"] = 'PR01'

        posicao["EMFPRAZOFLUXO"]= arquivo_operacoes["du"].iloc[linha]
        posicao["EMFVLREXPRISCO"]= arquivo_operacoes["mtm_mercado"].iloc[linha] if arquivo_operacoes["caracteristica"].iloc[linha]=="V" else arquivo_operacoes["mtm_info"].iloc[linha]
        posicao["EMFCODISIN"]= arquivo_operacoes["isin"].iloc[linha]
        posicao["EMFCODCUSTODIA"]= arquivo_operacoes["ativo"].iloc[linha]
        posicao["EMFMULTIPLOFATOR"]= 0 #Ajuste posterior para exposições com mais de 1 fator de risco
        posicao["EMFTXCONTRATADO"]= arquivo_operacoes["perc_index"].iloc[linha] if posicao["FTRCODIGO"] in ("TS1", "TD1") else 0
        posicao["EMFTXMERCADO"]= 100 if posicao["EMFTXCONTRATADO"]!=0 else 0
        posicao["TPFOPERADORDERIVATIVO"]= 0
        posicao["EMFVLRDERIVATIVO"]= 0
        posicao["EMFCODGRUPO"] = 0
        posicao["tipo_produto"] = arquivo_operacoes["produto"].iloc[linha]

        ## Ajuste tipo_relatorio: se for gerencial (G) não considerar papéis marcados na curva (Vencimento)
        if tipo_relatorio=="G" and arquivo_operacoes["caracteristica"].iloc[linha]=="V":
            posicao={}
        quaid_419.loc[linha] = pd.Series(posicao)
    
    ## Limpar linhas vazias
    quaid_419.dropna(how='all', inplace=True)

    ## Lista de ISINs
    lista_isin = quaid_419["EMFCODISIN"].unique()
     
    ## Filtrar os fluxos dos ISINs
    fluxos_isin = fluxos[fluxos["codigo_isin"].isin(lista_isin)]
     
    ## Substituicao Fluxos
    if len(fluxos_isin) > 0:
    
        compromissada = quaid_419[quaid_419['tipo_produto'].str.contains('compromissada')]
        quaid_419 = quaid_419[(quaid_419['tipo_produto']!="compromissada: titulo público")&(quaid_419['tipo_produto']!="compromissada - debênture")]
    
        temp = pd.merge(quaid_419[quaid_419["ATVCODIGO"]!="D0002"], fluxos, left_on = ["EMFCODISIN"], right_on =["codigo_isin"], how ="left")
        temp = temp.append(compromissada)
        temp["EMFVLREXPRISCO"]=np.where(np.isnan(temp["perc_mtm"]),temp["EMFVLREXPRISCO"],temp["perc_mtm"]*temp["EMFVLREXPRISCO"] )
        temp["EMFPRAZOFLUXO"]=np.where(np.isnan(temp["perc_mtm"]),temp["EMFPRAZOFLUXO"],temp["prazo_du"])
        temp["EMFPRAZOFLUXO"] = np.where(temp["tipo_produto"].isin(["ações"]),1,temp["EMFPRAZOFLUXO"])
        temp["EMFPRAZOFLUXO"] = np.where(temp["tipo_produto"].str.contains('compromissada'),1,temp["EMFPRAZOFLUXO"])
        temp["FTRCODIGO"] = np.where((temp["tipo_ativo"].isin(['CTF']))&(temp["indexador"].isin(['IPCA','IAP','IPC'])),'JI1',temp["FTRCODIGO"])
        temp["FTRCODIGO"] = np.where((temp["tipo_ativo"].isin(['CTF']))&(temp["indexador"].isin(['DI1','DI','CDI'])),'TD1',temp["FTRCODIGO"])
        temp["FTRCODIGO"] = np.where((temp["tipo_produto"].str.contains('compromissada'))&(temp["indexador"].isin(['PRE'])),'JJ1',temp["FTRCODIGO"])
        temp["EMFPRAZOFLUXO"] = np.where(temp['FTRCODIGO'].isin(['FF1']),1,temp["EMFPRAZOFLUXO"])
    
    ## Eliminar colunas do merge
        del temp['codigo_isin']
        del temp['perc_mtm']
        del temp['taxa_juros']
        del temp['tipo_ativo']
        del temp['prazo_du']
        del temp['data_bd']
        del temp['data_mtm']
        del temp['indexador']
    
        temp['EMFPRAZOFLUXO'] = np.where(temp['EMFCODISIN']=='BRSTNCLTN6W5',1,temp['EMFPRAZOFLUXO'])

    ## Eliminar linhas dos fluxos anteriores a data de referência
        temp['EMFPRAZOFLUXO'] = temp['EMFPRAZOFLUXO'].fillna(1)
        temp= temp[temp["EMFPRAZOFLUXO"]>0]
       
    ## Voltar linhas D0002
        temp = temp.append(quaid_419[quaid_419["ATVCODIGO"]=="D0002"], ignore_index=True)
        quaid_419 = temp
    
    ## Ajustar Futuros
    futuros = quaid_419[quaid_419["ATVCODIGO"] == "D0001"]
    
    isins = futuros["EMFCODISIN"].unique()
    cnpjs = futuros["EMFCNPJFUNDO"].unique()
    prazos = futuros["EMFPRAZOFLUXO"].unique()
    indexadores = futuros["FTRCODIGO"].unique()

    for cnpj in cnpjs:
        for isin in isins:
            for prazo in prazos:
                for indexador in indexadores:
                    lista = futuros[(futuros["EMFCODISIN"]==isin) & (futuros["EMFCNPJFUNDO"]==cnpj) & (futuros["EMFPRAZOFLUXO"]==prazo) & (futuros["FTRCODIGO"]==indexador)] ################ INSERI & (futuros["FTRCODIGO"]==indexador)
                    if len(lista) < 1:
                        pass
                    else:
                        valor_exprisco = sum(lista[lista.EMFCODCUSTODIA.notnull()]["EMFVLREXPRISCO"])
                        valor_ajuste = sum(lista[lista.EMFCODCUSTODIA.isnull()]["EMFVLREXPRISCO"])
                        linha_final = lista[lista.EMFCODCUSTODIA.notnull()]
                        linha_final["EMFVLREXPRISCO"]=valor_exprisco
                        linha_final["EMFVLRDERIVATIVO"]=valor_ajuste
                        linha_final["TPFOPERADORDERIVATIVO"] = "+" if valor_ajuste>0 else "-"
                        linha_final = linha_final.drop_duplicates(subset=["EMFVLREXPRISCO","EMFVLRDERIVATIVO","TPFOPERADORDERIVATIVO"])
                        futuros_ajustados = futuros_ajustados.append(linha_final)
                
    ## Substituir Futuros
    quaid_419 = quaid_419[quaid_419["ATVCODIGO"]!="D0001"]
    quaid_419 = quaid_419.append(futuros_ajustados)

    #Futuros de acoes
    if len(futuros_ajustados)>0:
        futuros_acoes = futuros_ajustados[futuros_ajustados["FTRCODIGO"]=="AA1"]
        if len(futuros_acoes)>0:
            futuros_acoes["FTRCODIGO"]="JJ1"
            futuros_acoes["EMFMULTIPLOFATOR"]= 1 
            for i in range(len(futuros_acoes["FTRCODIGO"])):
                futuros_acoes["TPFOPERADOR"][i]="+" if futuros_acoes["TPFOPERADOR"][i]=="-" else "-"
            quaid_419 = quaid_419.append(futuros_acoes)
        
        #Futuros de dólar
        futuros_dolar = futuros_ajustados[futuros_ajustados["FTRCODIGO"]=="ME1"]
        if len(futuros_dolar)>0:
            futuros_dolar["FTRCODIGO"]="JJ1"
            futuros_dolar["EMFMULTIPLOFATOR"]= 1 
            for i in range(len(futuros_dolar["FTRCODIGO"])):
                futuros_dolar["TPFOPERADOR"][i] = "+" if futuros_dolar["TPFOPERADOR"][i] == "-" else "-"
            quaid_419 = quaid_419.append(futuros_dolar)

    ## Agregação por CNPJ,Isin,Prazo
    #Preenchecom 14 0 o cnpj
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isin(['0000000000None']),'00000000000000',quaid_419["EMFCNPJFUNDO"])
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isin([0]),'00000000000000',quaid_419["EMFCNPJFUNDO"])
    quaid_419["EMFCNPJFUNDO"] = np.where(quaid_419["EMFCNPJFUNDO"].isnull(),'00000000000000',quaid_419["EMFCNPJFUNDO"])
    
    #Preenchecom 12 0 o isin
    quaid_419["EMFCODISIN"] = quaid_419["EMFCODISIN"].fillna('000000000000')
    
    #SEPARAÇÃO CAIXA O QUE NAO TEM ISIN DO QUE TEM
    semisin = quaid_419[quaid_419['tipo_produto'].isin(['caixa','valores a pagar','valor das cotas a resgatar','valores a receber'])].copy()
    comisin = quaid_419[~quaid_419['tipo_produto'].isin(['caixa','valores a pagar','valor das cotas a resgatar','valores a receber'])].copy()
    
    #COM ISIN
    df_lista = comisin[['EMFCNPJFUNDO','EMFCODISIN','EMFPRAZOFLUXO','EMFVLREXPRISCO','EMFVLRDERIVATIVO']].groupby(['EMFCNPJFUNDO','EMFCODISIN','EMFPRAZOFLUXO']).agg('sum')
    df_lista = df_lista.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    
    df_teste = comisin.copy()

    del df_teste['EMFSEQ']
    del df_teste['EMFVLREXPRISCO']
    del df_teste['EMFVLRDERIVATIVO']

    df_teste = df_teste.drop_duplicates(subset=['EMFCNPJFUNDO','EMFCODISIN','EMFPRAZOFLUXO'],take_last=True)
    df_teste = pd.merge(df_teste, df_lista, right_on=['EMFCNPJFUNDO','EMFCODISIN','EMFPRAZOFLUXO'],left_on=['EMFCNPJFUNDO','EMFCODISIN','EMFPRAZOFLUXO'],how='left')
    
    #Junta com o que não tem isin
    df_teste = df_teste.append(semisin)
    quaid_419 = df_teste.copy()
    
    #Força o TPFOPERADOR
    quaid_419['TPFOPERADOR'] = np.where(quaid_419['EMFVLREXPRISCO']>=0,"+","-")
    quaid_419['TPFOPERADORDERIVATIVO'] = np.where(quaid_419['EMFVLRDERIVATIVO']>=0,"+","-")

    ## Muda o fator de risco para fundos que foram abertos
    quaid_419['ATVCODIGO'] = np.where((quaid_419['tipo_produto'].isin(['fundo']))&(~quaid_419['FTRCODIGO'].isin(['FF1'])),'A1001',quaid_419['ATVCODIGO'])

    ## Ajusta o TCTCODIGO
    quaid_419['TCTCODIGO'] = np.where(quaid_419['EMFCNPJFUNDO'].isin(['00000000000000']),'01','02')

    ## Ajusta o TPFOPERADORDERIVATIVO
    quaid_419['TPFOPERADORDERIVATIVO'] = np.where(~quaid_419['ATVCODIGO'].isin(['D0001', 'D1001', 'D1002', 'D2001', 'D2002', 'D3001']),'0', quaid_419['TPFOPERADORDERIVATIVO'])

    ## Ajustes finais
    quaid_419["EMFVLREXPRISCO"] = quaid_419["EMFVLREXPRISCO"].abs()
    quaid_419["EMFVLRDERIVATIVO"] = quaid_419["EMFVLRDERIVATIVO"].abs()
    
    ## Reordenar contagem de linhas
    quaid_419["EMFSEQ"] = range(1,len(quaid_419["FTRCODIGO"])+1,1)
    
    ## Inserir data_bd
    quaid_419["data_bd"] = datetime.datetime.now()
    
    ## Inserir identificadores do relatório
    quaid_419["id_relatorio_qo"]=id_relatorio_qo
    quaid_419["tipo_relatorio"]= tipo_relatorio

    id_relatorio_quaid419 = pd.read_sql_query('SELECT max(id_relatorio_quaid419) from projeto_inv.quaid_419', connection)
    
    id_relatorio_quaid419=id_relatorio_quaid419["max(id_relatorio_quaid419)"][0]
    try:
        id_relatorio_quaid419+=1
    except:
        id_relatorio_quaid419=1
        
    quaid_419["id_relatorio_quaid419"]= id_relatorio_quaid419
        
    ## Substituir NaN do prazo por 1 e outros por 0
    quaid_419["EMFPRAZOFLUXO"].fillna(1, inplace=True)
    quaid_419.fillna(0, inplace=True)

    logger.info("Salvando tabela quaid419 no banco de dados")
    pd.io.sql.to_sql(quaid_419, name='quaid_419', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)
    
    #horario_processo = datetime.datetime.today()
    aux = quaid_419[['id_relatorio_qo','ENTCODIGO','tipo_relatorio','id_relatorio_quaid419']].copy()
    aux['data_bd'] = horario_processo    
    aux = aux.drop_duplicates()

    logger.info("Salvando tabela count_quadros no banco de dados")
    pd.io.sql.to_sql(aux, name='count_quadros', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)    

    #Fecha conexão
    connection.close()