def matriz_gerencial_2(retornos):

    import pymysql as db
    import datetime
    import numpy as np
    import pandas as pd
    import logging

    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from var.scripts.matriz_gerencial.definicao_lambda import definicao_lambda
    from var.scripts.matriz_gerencial.definicao_nome import definicao_nome

    #Define variáveis:
    logger = logging.getLogger(__name__)
    save_path = full_path_from_database("get_output_var")
    dt_base = get_data_ultimo_dia_util_mes_anterior()
    #data_final = data_final = str(dt_base[0])+'-'+str(dt_base[1])+'-'+str(dt_base[2])
    data_final = '2016-11-30'
    dt_base = dt_base[0] + dt_base[1] + dt_base[2]
    dt_base = '20161130'
    data_inicial = "2010-03-31"
    lambda_geral = definicao_lambda('outro') #lambda_geral = 0.95

    # Conecta no DB
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    logger.info("Tratando dados")
    ## Cálculo da matriz de correlação
    matriz_correlacao = retornos.corr()
    matriz_correlacao.fillna(0, inplace=True)

    variancias_manual = pd.read_excel(full_path_from_database("get_output_var")+'variancias_'+dt_base+'.xlsx')

    #COMO QUE EU COLOCO NO FORMATO QUE ESTAVAM VINDO AS VARIANCIAS ANTES?
    variancias = variancias_manual.values
    desvio_padrao = np.sqrt(variancias)
    desvio_padrao = np.squeeze(desvio_padrao)

    ## Cálculo da matriz de covariância
    matriz_diagonal = np.diag(desvio_padrao)
    matriz_covariancias = np.dot(matriz_diagonal, np.dot(matriz_correlacao, matriz_diagonal))

    ## Salvar no banco de dados
    base_final = pd.DataFrame()

    for linha in range(len(retornos.columns)):
        for coluna in range(linha+1):
            resultado ={}
            if isinstance(retornos.columns[linha], tuple) and len(retornos.columns[linha])>2:
                resultado["linha"] = definicao_nome(retornos.columns[linha][1])+"_"+str(retornos.columns[linha][2])
            else:
                resultado["linha"] = definicao_nome(retornos.columns[linha])
            if isinstance(retornos.columns[coluna], tuple) and len(retornos.columns[coluna])>2:
                resultado["coluna"] = definicao_nome(retornos.columns[coluna][1])+"_"+str(retornos.columns[coluna][2])
            else:
                resultado["coluna"] = definicao_nome(retornos.columns[coluna])
            resultado["valor"] = matriz_covariancias[linha][coluna]
            base_final = base_final.append(resultado, ignore_index=True)

    max(base_final["valor"])
    base_final["data_bd"] = datetime.datetime.now()
    base_final["data_inicial"] = data_inicial
    base_final["data_final"] = data_final
    base_final["horizonte"] = "dia"
    base_final["lambda"] = lambda_geral

    # TESTE SE VETOR VEM ZERADO
    ## Inserir "matriz_id"


    matriz_id = pd.read_sql_query('SELECT max(matriz_id) as matriz_id FROM matriz_gerencial', connection)
    if matriz_id['matriz_id'][0]==None:
        matriz_id_valor = 0
    else:
        matriz_id_valor = matriz_id.get_value(0,'matriz_id').astype(int)+1

    base_final["matriz_id"] = matriz_id_valor

    ### Salvar no BD
    logger.info("Salvando base de dados")
    pd.io.sql.to_sql(base_final, name='matriz_gerencial', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)

    new_matriz_id = pd.read_sql_query('select matriz_id from matriz_gerencial where data_bd = ( SELECT MAX(data_bd) from matriz_gerencial	) group by matriz_id',connection)
    new_matriz_id = new_matriz_id['matriz_id'].iloc[0]

    cur = connection.cursor(db.cursors.DictCursor)
    query = 'update global_variables set variable_value = '+str(new_matriz_id)+' where variable_name = '+ '\'matriz_id\''

    logger.info("Atualizando matriz_id na base de dados")
    cur.execute(query)
    connection.commit()
    logger.info("Base de dados atualizada com sucesso")
    connection.close()

    base_final.to_excel(save_path+'matriz_gerencial_'+str(data_final)+'.xlsx')
    logger.info("Arquivo Matriz Gerencial salvo com sucesso")
    retornos.to_excel(save_path+'retornos_mensal_'+str(data_final)+'.xlsx')
    logger.info("Arquivo Retornos mensais salvo com sucesso")