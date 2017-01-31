def var_gerencial(id_relatorio_quaid419,relatorio_tipo,dtbase):

    #id_relatorio_quaid419 ='3696'
    #relatorio_tipo='normal'

    import pandas as pd
    import numpy as np
    import datetime
    import pymysql as db
    import re
    import logging
    import math

    from pandas import ExcelWriter
    from scipy.stats import norm
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import get_global_var
    from var.scripts.relatorio_encadeado.var_gerencial.value_at_risk import value_at_risk

    matriz_id = 30
    #matriz_id = get_global_var("matriz_id")
    #id_relatorio_quaid419 = get_global_var("id_qua419") #arg1
    fator_confianca = 0.99
    horizonte_em_dias = 1
    t = math.sqrt(horizonte_em_dias)
    #relatorio_tipo="" #arg2

    logger = logging.getLogger(__name__)

    #Diretório de save de planilhas
    save_path = full_path_from_database("get_output_var")

    writer = ExcelWriter(save_path+'/var_'+dtbase+'.xlsx')

    fc_normal = norm.ppf(fator_confianca)
    fc_stress = norm.pdf(fc_normal)/(1-fator_confianca)

    if relatorio_tipo == "estressado":
        fc_normal = fc_stress

    horario = datetime.datetime.now()

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")

    logger.info("Conexão com DB executada com sucesso")

    ### Dados matriz regulatória
    ##################################LIMIT 100################################
    #dados_matriz = pd.read_sql_query('SELECT * from projeto_inv.matriz_gerencial where matriz_id='+str(matriz_id), connection) #CORRETO
    #+' limit 10'
    dados_matriz = pd.read_sql_query('SELECT * from projeto_inv.matriz_gerencial where matriz_id='+str(matriz_id)+' limit 100', connection) #TESTE
    logger.info("Leitura do banco de dados executada com sucesso")

    #Salva planilha
    dados_matriz.to_excel(save_path+'matriz.xlsx')
    logger.info("matriz.xlsx salva com sucesso")

    ## Reconstrução da Matriz

    #Pega os valores únicos da coluna "linha" e cria uma coluna para cada valor distinto.
    matriz_reconstruida = pd.DataFrame(columns=dados_matriz["linha"].unique())

    #Pega os valores únicos da coluna "linha" e cria uma linha para cada valor distinto.
    matriz_reconstruida[matriz_reconstruida.columns[0]] = matriz_reconstruida.columns

    #Popula cada linha e coluna com os valores da tabela "dados_matriz" na nova estrutura "matriz_reconstruida"
    for coluna in matriz_reconstruida.columns:
        for linha in range(len(matriz_reconstruida.columns)):
            try:
                matriz_reconstruida[coluna].iloc[linha]=dados_matriz[(dados_matriz["coluna"]==coluna) & (dados_matriz["linha"]==matriz_reconstruida.columns[linha])]["valor"].values[0]
            except:
                matriz_reconstruida[coluna].iloc[linha]=dados_matriz[(dados_matriz["linha"]==coluna) & (dados_matriz["coluna"]==matriz_reconstruida.columns[linha])]["valor"].values[0]

    ### Vetor de exposicoes
    ##################################LIMIT 100################################
    #+' limit 10'
    query_total="SELECT * FROM projeto_inv.vetor_exposicoes where id_relatorio_quaid419="+str(id_relatorio_quaid419)+" and data_bd= (SELECT MAX(data_bd) FROM  projeto_inv.vetor_exposicoes where id_relatorio_quaid419="+str(id_relatorio_quaid419)+" ) limit 100"
    query_sep="SELECT * FROM projeto_inv.vetor_exposicoes_sep where id_relatorio_quaid419="+str(id_relatorio_quaid419)+" and data_bd= (SELECT MAX(data_bd) FROM  projeto_inv.vetor_exposicoes_sep where id_relatorio_quaid419="+str(id_relatorio_quaid419)+" ) limit limit 100"

    vetor_exposicoes_total = pd.read_sql_query(query_total, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    vetor_separado = pd.read_sql_query(query_sep, connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #Ajuste por causa da saida da bd que é crazy
    #+' limit 10'
    quaid_419_aux = pd.read_sql_query('SELECT * from projeto_inv.quaid_419 where id_relatorio_quaid419='+str(id_relatorio_quaid419)+' LIMIT 100', connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    if len(quaid_419_aux[quaid_419_aux['FTRCODIGO']=='AA1']):

        vetor_separado_temp = vetor_separado[(vetor_separado['vertice']=='IBOV_1')&(~vetor_separado['categoria_alocacao'].isin(['fundo','Cotas']))].copy()
        vetor_separado_temp['valor_exposicao'] = np.where(vetor_separado_temp['categoria_alocacao'].isin(['ações','RV']),0,vetor_separado_temp['valor_exposicao'])
        vetor_separado = vetor_separado[(vetor_separado['vertice']!='IBOV_1')&(~vetor_separado['categoria_alocacao'].isin(['fundo','Cotas']))]
        if vetor_separado_temp['valor_exposicao'].sum()!=0:
            vetor_separado = vetor_separado.append(vetor_separado_temp)

        vetor_separado_temp['valor_exposicao'] = np.where(vetor_separado_temp['categoria_alocacao'].isin(['ações','RV']),1,vetor_separado_temp['valor_exposicao'])

        #+' limit 10'
        quaid_419_aux = pd.read_sql_query('SELECT * from projeto_inv.quaid_419 where id_relatorio_quaid419='+str(id_relatorio_quaid419)+' limit 100', connection)
        logger.info("Leitura do banco de dados executada com sucesso")

        #Fecha conexão
        connection.close()

        quaid_419_aux = quaid_419_aux[quaid_419_aux['FTRCODIGO']=='AA1']
        quaid_419_aux['EMFCODCUSTODIA'] = quaid_419_aux['EMFCODCUSTODIA'] + '_1'

        x = quaid_419_aux.groupby(['EMFCODCUSTODIA']).agg(['sum'])
        x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        x1 = pd.DataFrame(columns=['vertice','valor_exposicao'])
        x1['vertice'] = x['EMFCODCUSTODIA']
        x1['valor_exposicao'] = x['EMFVLREXPRISCO']

        lista_acoes = quaid_419_aux['EMFCODCUSTODIA'].unique()

        for i in lista_acoes:
            vetor_separado_temp['vertice'] = i
            vetor_separado_temp['valor_exposicao'] = vetor_separado_temp['valor_exposicao']*x1['valor_exposicao'][x1['vertice']==i].iloc[0]
            vetor_separado = vetor_separado.append(vetor_separado_temp)
            vetor_separado_temp['valor_exposicao'] = np.where(vetor_separado_temp['categoria_alocacao'].isin(['ações', 'RV']),1, vetor_separado_temp['valor_exposicao'])

        vetor_exposicoes_total.to_excel(save_path+'vetor_exposicoes_conso.xlsx')
        logger.info("Vetor exposições consolidado salvo com sucesso")

    vetor_separado.to_excel(save_path+'vetor_exp_sep.xlsx')
    logger.info("Vetor exposições separado salvo com sucesso")

    ## Cálculo do VaR para posicao Total

    for i in range(len(vetor_exposicoes_total["vertice"])):
        vetor_exposicoes_total["vertice"].iloc[i] = re.sub(r"_1\b","", vetor_exposicoes_total["vertice"].iloc[i])
        vetor_exposicoes_total["vertice"].iloc[i] = re.sub(r"\bCCAMBIAL\b","DOL", vetor_exposicoes_total["vertice"].iloc[i])

    ## Fundos sem cota alocados em IBOV
    lista_fundos_sem_cota = []
    for i in range(len(vetor_exposicoes_total["vertice"])):
        if (vetor_exposicoes_total["vertice"].iloc[i] not in matriz_reconstruida.columns):
            if vetor_exposicoes_total["vertice"].iloc[i] not in ['IGPM','DOL','COMMODITIES','IPCA','TR','DOL']:
                lista_fundos_sem_cota.append(vetor_exposicoes_total["vertice"].iloc[i])

    if len(lista_fundos_sem_cota) > 0:
        index_ibov = vetor_exposicoes_total[vetor_exposicoes_total["vertice"] == "IBOV"].index.tolist()[0]

        for i in range(len(vetor_exposicoes_total["vertice"])):
            if vetor_exposicoes_total["vertice"].iloc[i] in lista_fundos_sem_cota:
                vetor_exposicoes_total["valor_exposicao"].iloc[index_ibov] += \
                vetor_exposicoes_total["valor_exposicao"].iloc[i]

    vetor_exposicoes_total.to_excel(save_path+'vetor_exposicoes_conso.xlsx')
    logger.info("Vetor exposições consolidado salvo com sucesso")

    vetor_ordenado=[]
    for vertice in matriz_reconstruida.columns:
        try:
            vetor_ordenado.append(vetor_exposicoes_total[vetor_exposicoes_total["vertice"]==vertice]["valor_exposicao"].values[0])
        except:
            vetor_ordenado.append(0)

    matriz_reconstruida.fillna(0, inplace=True)

    ### Cálculo do Value at Risk
    #fc = scipy.stats.norm.ppf(fator_confianca)
    #t = math.sqrt(horizonte_em_dias)

    var_total = value_at_risk (np.array(vetor_ordenado), matriz_reconstruida, fc_normal,t)

    matriz_reconstruida.to_excel(save_path+'matriz.xlsx')
    logger.info("Matriz.xlsx salva com sucesso")

    ### Salvar no BD
    resultados_total = pd.DataFrame()

    ## Var Total
    dados_var ={}
    dados_var["var"]= var_total[0]
    dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
    dados_var["vigencia_matriz"] = str(matriz_id)
    dados_var["tipo_relatorio"]="G"
    dados_var["horizonte_tempo"]= horizonte_em_dias
    dados_var["nivel_confianca"] = fator_confianca
    dados_var["tipo_var"]="Total"
    dados_var["vertice"]="Total"
    dados_var["tipo_alocacao"]="Total"
    dados_var["tipo_segmento"]="Total"
    resultados_total= resultados_total.append(dados_var, ignore_index=True)

    ## VaR Marginal
    for item in range(len(var_total[1])):
        dados_var ={}
        dados_var["var"]= var_total[1][item]
        dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
        dados_var["vigencia_matriz"] = str(matriz_id)
        dados_var["tipo_relatorio"]="G"
        dados_var["horizonte_tempo"]= horizonte_em_dias
        dados_var["nivel_confianca"] = fator_confianca
        dados_var["tipo_var"]="Marginal"
        dados_var["vertice"]=matriz_reconstruida.columns[item]
        dados_var["tipo_alocacao"]="Total"
        dados_var["tipo_segmento"]="Total"
        resultados_total= resultados_total.append(dados_var, ignore_index=True)

    ## VaR Componente
    for item in range(len(var_total[2])):
        dados_var ={}
        dados_var["var"]= var_total[2][item]
        dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
        dados_var["vigencia_matriz"] = str(matriz_id)
        dados_var["tipo_relatorio"]="G"
        dados_var["horizonte_tempo"]= horizonte_em_dias
        dados_var["nivel_confianca"] = fator_confianca
        dados_var["tipo_var"]="Componente"
        dados_var["vertice"]=matriz_reconstruida.columns[item]
        dados_var["tipo_alocacao"]="Total"
        dados_var["tipo_segmento"]="Total"
        resultados_total= resultados_total.append(dados_var, ignore_index=True)

    ### Colocar data_bd
    resultados_total["data_bd"] = horario

    ##Colocar o tipo de relatório NORMAL ou ESTRESSADO
    resultados_total['norm_stress'] = relatorio_tipo

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")

    ## Salvar no bando de dados
    pd.io.sql.to_sql(resultados_total, name='var', con=connection, if_exists="append", flavor='mysql', index=0, chunksize=5000)

    #Fecha conexão
    connection.close()

    resultados_total.to_excel(writer,'resultados_total')
    logger.info("Resultados_total.xlsx salvo com sucesso")

    ### Cálculo do VaR Stressado
    resultados_separado = pd.DataFrame()

    for i in range(len(vetor_separado["vertice"])):
        vetor_separado["vertice"].iloc[i] = re.sub(r"_1\b","", vetor_separado["vertice"].iloc[i])
        vetor_separado["vertice"].iloc[i] = re.sub(r"\bCCAMBIAL\b","DOL", vetor_separado["vertice"].iloc[i])

    for tipo_alocacao in vetor_separado["tipo_alocacao"].unique():
        vetor_filtrado = vetor_separado[vetor_separado["tipo_alocacao"]==tipo_alocacao]
        for categoria in vetor_filtrado["categoria_alocacao"].unique():
            vetor_filtrado2 = vetor_filtrado[vetor_filtrado["categoria_alocacao"]==categoria]

            ## Fundos sem cota alocados em IBOV
            lista_fundos_sem_cota = []
            if len(lista_fundos_sem_cota)>0:
                for i in range(len(vetor_filtrado2["vertice"])):
                    if vetor_filtrado2["vertice"].iloc[i] not in matriz_reconstruida.columns:
                        lista_fundos_sem_cota.append(vetor_filtrado2["vertice"].iloc[i])

                index_ibov = vetor_filtrado2[vetor_filtrado2["vertice"]=="IBOV"].index.tolist()[0]

                for i in range(len(vetor_filtrado2["vertice"])):
                    if vetor_filtrado2["vertice"].iloc[i] in lista_fundos_sem_cota:
                        vetor_filtrado2["valor_exposicao"].iloc[index_ibov]+=vetor_filtrado2["valor_exposicao"].iloc[i]

            vetor_ordenado=[]
            for vertice in matriz_reconstruida.columns:
                try:
                    vetor_ordenado.append(vetor_filtrado2[vetor_filtrado2["vertice"]==vertice]["valor_exposicao"].values[0])
                except:
                    vetor_ordenado.append(0)
            var_parcial=value_at_risk (np.array(vetor_ordenado), matriz_reconstruida,fc_normal,t)

    ### Salvar no BD
            ## Var Total
            dados_var ={}
            dados_var["var"]= var_parcial[0]
            dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
            dados_var["vigencia_matriz"] = str(matriz_id)
            dados_var["tipo_relatorio"]="G"
            dados_var["horizonte_tempo"]= horizonte_em_dias
            dados_var["nivel_confianca"] = fator_confianca
            dados_var["tipo_var"]="Total"
            dados_var["vertice"]="Total"
            dados_var["tipo_alocacao"]=tipo_alocacao
            dados_var["tipo_segmento"]=categoria
            resultados_separado= resultados_separado.append(dados_var, ignore_index=True)

            ## VaR Marginal
            for item in range(len(var_parcial[1])):
                dados_var ={}
                dados_var["var"]= var_parcial[1][item]
                dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
                dados_var["vigencia_matriz"] = str(matriz_id)
                dados_var["tipo_relatorio"]="G"
                dados_var["horizonte_tempo"]= horizonte_em_dias
                dados_var["nivel_confianca"] = fator_confianca
                dados_var["tipo_var"]="Marginal"
                dados_var["vertice"]=matriz_reconstruida.columns[item]
                dados_var["tipo_alocacao"]=tipo_alocacao
                dados_var["tipo_segmento"]=categoria
                resultados_separado= resultados_separado.append(dados_var, ignore_index=True)

            ## VaR Componente
            for item in range(len(var_parcial[2])):
                dados_var ={}
                dados_var["var"]= var_parcial[2][item]
                dados_var["id_relatorio_quaid419"]=id_relatorio_quaid419
                dados_var["vigencia_matriz"] = str(matriz_id)
                dados_var["tipo_relatorio"]="G"
                dados_var["horizonte_tempo"]= horizonte_em_dias
                dados_var["nivel_confianca"] = fator_confianca
                dados_var["tipo_var"]="Componente"
                dados_var["vertice"]=matriz_reconstruida.columns[item]
                dados_var["tipo_alocacao"]=tipo_alocacao
                dados_var["tipo_segmento"]=categoria
                resultados_separado= resultados_separado.append(dados_var, ignore_index=True)

    ### Colocar data_bd
    resultados_separado["data_bd"] = datetime.datetime.now()

    comparacao_total = resultados_total[resultados_total["tipo_var"]=="Total"]["var"]
    comparacao_segmento = resultados_separado[(resultados_separado["tipo_var"]=="Total") & (resultados_separado["tipo_alocacao"]=="segmento")]
    comparacao_produtos = resultados_separado[(resultados_separado["tipo_var"]=="Total") & (resultados_separado["tipo_alocacao"]=="produto")]

    resultados_separado = resultados_separado.drop_duplicates(subset=['tipo_segmento','tipo_segmento','tipo_var','var','vertice'],take_last=True)

    ### Colocar data_bd
    resultados_separado["data_bd"] = horario

    ##Colocar o tipo de relatório NORMAL ou ESTRESSADO
    resultados_separado['norm_stress'] = relatorio_tipo

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando base de dados")

    ## Salvar no bando de dados
    pd.io.sql.to_sql(resultados_separado, name='var', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)

    #Fecha conexão
    connection.close()

    resultados_separado.to_excel(writer,'var_separado')
    logger.info("var_separado.xlsx salvo com sucesso")

    writer.save()