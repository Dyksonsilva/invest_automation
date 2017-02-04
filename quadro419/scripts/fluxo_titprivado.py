def fluxo_titprivado():

    import pandas as pd
    import datetime
    import numpy as np
    import pymysql as db
    import logging
    from pandas.tseries.offsets import DateOffset
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    logger = logging.getLogger(__name__)

    # Diretório de save de planilhas
    xlsx_path_fluxo_debenture = full_path_from_database("get_output_quadro419") + 'controle_fluxo_debentures.xlsx'

    #Diretório de save de planilhas
    save_path_fluxo_A = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_A.xlsx'
    save_path_fluxo_B = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_B.xlsx'
    save_path_fluxo_M = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_M.xlsx'
    save_path_fluxo_Q = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_Q.xlsx'
    save_path_fluxo_S = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_S.xlsx'
    save_path_fluxo_W = full_path_from_database("get_output_quadro419") + 'bmf_fluxo_titprivado_W.xlsx'
    save_path_fluxo_titprivado = full_path_from_database("get_output_quadro419") + 'fluxo_titprivado.xlsx'

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    #dtbase = ['2016','11','30']
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    #2 - Cria conexão e importação: base de dados fluxo_titprivado

    #Conexão com Banco de Dados
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv'
, use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * FROM projeto_inv.titprivado_caracteristicas WHERE data_expiracao > ' + '"' + dtbase_concat + '";'
    bmf_numeraca = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    #3 - Preparação da base
    logger.info("Tratando dados")

    #Seleciona dados da última carga da data de relatório
    bmf_numeraca['dtrel'] = bmf_numeraca['id_papel'].str.split('_')
    bmf_numeraca['dtrel'] = bmf_numeraca['dtrel'].str[0]

    bmf_numeraca = bmf_numeraca[bmf_numeraca.dtrel == dtbase_concat].copy()
    bmf_numeraca = bmf_numeraca[bmf_numeraca.data_bd == max(bmf_numeraca.data_bd)]

    del bmf_numeraca['dtrel']

    bmf_numeraca = bmf_numeraca.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='').copy()

    titprivado = bmf_numeraca.copy()

    #Guarda lista de todos os papéis para verificar no final se há papel faltando no fluxo
    lista_all = bmf_numeraca[['id_papel','codigo_isin']].copy()
    lista_all = lista_all.drop_duplicates(subset=['id_papel'])

    #excluir debêntures
    titprivado = titprivado[titprivado.tipo_ativo !='DBS']
    #excluir debêntures conversíveis em ações ordinárias
    titprivado = titprivado[titprivado.tipo_ativo !='DBO']
    #excluir debêntures conversíveis em ações preferenciais
    titprivado = titprivado[titprivado.tipo_ativo !='DBP']
    #excluir debênures miscelâneas
    titprivado = titprivado[titprivado.tipo_ativo !='DBM']

    #excluir títulos puúblicos
    titprivado = titprivado[titprivado.tipo_ativo !='NTB']
    titprivado = titprivado[titprivado.tipo_ativo !='NTC']
    titprivado = titprivado[titprivado.tipo_ativo !='NTF']
    titprivado = titprivado[titprivado.tipo_ativo !='LF']
    titprivado = titprivado[titprivado.tipo_ativo !='LTN']
    titprivado = titprivado[titprivado.tipo_ativo !='NI']
    titprivado = titprivado[titprivado.tipo_ativo !='NTP']
    titprivado = titprivado[titprivado.tipo_ativo !='NTA']
    titprivado = titprivado[titprivado.tipo_ativo !='NTD']
    titprivado = titprivado[titprivado.tipo_ativo !='NTE']
    titprivado = titprivado[titprivado.tipo_ativo !='NTH']
    titprivado = titprivado[titprivado.tipo_ativo !='NTJ']
    titprivado = titprivado[titprivado.tipo_ativo !='NTL']
    titprivado = titprivado[titprivado.tipo_ativo !='NT']
    titprivado = titprivado[titprivado.tipo_ativo !='NTM']
    titprivado = titprivado[titprivado.tipo_ativo !='NTR']
    titprivado = titprivado[titprivado.tipo_ativo !='NTS']
    titprivado = titprivado[titprivado.tipo_ativo !='NTT']
    titprivado = titprivado[titprivado.tipo_ativo !='NTU']

    #existem sujeiras na base de dados em relação a data de vencimento (expiracao), os quais são acima de 2100
    ano = datetime.datetime.now().year
    prazo_maximo = 80 + ano

    problema_cadastro = titprivado[titprivado.ano_expiracao.astype(float) > prazo_maximo]
    titprivado = titprivado[titprivado.ano_expiracao.astype(float) < prazo_maximo]

    titprivado = titprivado.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    titprivado = titprivado[['codigo_isin','codigo_cetip','tipo_ativo','data_emissao','data_expiracao','valor_nominal','taxa_juros','indexador','percentual_indexador','cod_frequencia_juros','data_primeiro_pagamento_juros','id_bmf_numeraca','flag','id_papel','dtoperacao']].copy()

    #Caso o papel tenha um código de frequência de juros, preenche a primeira data de pagamento com a primeira data depois do periodo considerando o cod_frequencia_juros

    titprivado['data_emissao'] = pd.to_datetime(titprivado['data_emissao'])
    titprivado['data_primeiro_pagamento_juros'] = pd.to_datetime(titprivado['data_primeiro_pagamento_juros'])
    titprivado['data_expiracao'] = pd.to_datetime(titprivado['data_expiracao'])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'B')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(years=2)

    print(titprivado['data_primeiro_pagamento_juros'][idx])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'A')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(years=1)

    print(titprivado['data_primeiro_pagamento_juros'][idx])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'S')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(months=6)

    print(titprivado['data_primeiro_pagamento_juros'][idx])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'Q')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(months=3)

    print(titprivado['data_primeiro_pagamento_juros'][idx])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'M')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(months=1)

    print(titprivado['data_primeiro_pagamento_juros'][idx])

    idx = titprivado[
        (titprivado.data_primeiro_pagamento_juros.isnull()) & (titprivado.cod_frequencia_juros == 'W')].index.tolist()
    for i in idx:
        titprivado['data_primeiro_pagamento_juros'][idx] = titprivado['data_emissao'][idx] + DateOffset(days=7)

    #A - ANUAL; B - BI-ANUAL; M – MENSAL; N - NÃO APLICÁVEL; Q – TRIMESTRAL; S – SEMESTRAL; W - SEMANAL X – OUTROS

    #Trasnforma papéis sem data de expiração em BULLET
    titprivado['data_primeiro_pagamento_juros'][titprivado.data_primeiro_pagamento_juros.isnull()] = titprivado['data_expiracao'][titprivado.data_primeiro_pagamento_juros.isnull()]

    titprivado['data_primeiro_pagamento_juros'] = pd.to_datetime(titprivado['data_primeiro_pagamento_juros']).dt.date
    titprivado['data_emissao'] = pd.to_datetime(titprivado['data_emissao']).dt.date
    titprivado['data_expiracao'] = pd.to_datetime(titprivado['data_expiracao']).dt.date

    #Correção indexador
    titprivado['indexador'][titprivado.indexador=='IAP'] = 'IPCA'
    titprivado['indexador'][titprivado.indexador=='CDI'] = 'DI1'

    titprivado['cod_frequencia_juros'][titprivado.data_expiracao==titprivado.data_primeiro_pagamento_juros] = None

    bmf_fluxo_titprivado_A = pd.DataFrame(columns=['codigo_isin',
                                                 'codigo_cetip',
                                                 'tipo_ativo',
                                                 'data_emissao',
                                                 'data_expiracao',
                                                 'valor_nominal',
                                                 'taxa_juros',
                                                 'indexador',
                                                 'percentual_indexador',
                                                 'cod_frequencia_juros',
                                                 'data_primeiro_pagamento_juros',
                                                 'id_bmf_numeraca',
                                                 'data_bd',
                                                 'dt_ref',
                                                 'flag',
                                                 'id_papel',
                                                 'dtoperacao'])

    bmf_fluxo_titprivado_B = bmf_fluxo_titprivado_A.copy()
    bmf_fluxo_titprivado_S = bmf_fluxo_titprivado_A.copy()
    bmf_fluxo_titprivado_Q = bmf_fluxo_titprivado_A.copy()
    bmf_fluxo_titprivado_M = bmf_fluxo_titprivado_A.copy()
    bmf_fluxo_titprivado_W = bmf_fluxo_titprivado_A.copy()

    data_bd=datetime.datetime.now()

    #4 - Gera fluxo de papéis cod_frequencia_juros = "A": ANUAL

    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'A') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 12
        fatordiario = 0

        #Preenche o fluxo
        bmf_fluxo_titprivado_A.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_A.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1


    bmf_fluxo_titprivado_A.to_excel(save_path_fluxo_A)

    #5 - Gera fluxo de papéis cod_frequencia_juros = "B": BIANUAL
    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'B') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 24
        fatordiario = 0

        #Preenche o fluxo
        bmf_fluxo_titprivado_B.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_B.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1

    bmf_fluxo_titprivado_B.to_excel(save_path_fluxo_B)

    #6 - Gera fluxo de papéis cod_frequencia_juros = "M": MENSAL
    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'M') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 1
        fatordiario = 0

        #Preenche o fluxo
        bmf_fluxo_titprivado_M.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_M.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1

    bmf_fluxo_titprivado_M.to_excel(save_path_fluxo_M)

    #7 - Gera fluxo de papéis cod_frequencia_juros = "Q": TRIMESTRAL
    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'Q') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 3
        fatordiario = 0

        #Preenche o fluxo
        bmf_fluxo_titprivado_Q.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_Q.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1

    bmf_fluxo_titprivado_Q.to_excel(save_path_fluxo_Q)

    #8 - Gera fluxo de papéis cod_frequencia_juros = "S": SEMESTRAL
    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'S') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 6
        fatordiario = 0

        #Preenche o fluxo
        bmf_fluxo_titprivado_S.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_S.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1

    bmf_fluxo_titprivado_S.to_excel(save_path_fluxo_S)

    #9 - Gera fluxo de papéis cod_frequencia_juros = "W": SEMANAL
    titprivado_aux = titprivado[(titprivado['cod_frequencia_juros'] == 'W') & (titprivado.data_primeiro_pagamento_juros!=titprivado.data_expiracao)].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    i1 = 0
    for i in range(0,len(titprivado_aux)):
        qtde=0
        vencto = pd.to_datetime(titprivado_aux['data_primeiro_pagamento_juros'][i])

        #Criar o intervalo de tempo
        fatormensal = 0
        fatordiario = 7

        #Preenche o fluxo
        bmf_fluxo_titprivado_W.loc[i+i1]=[titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i],titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,titprivado_aux['data_expiracao'][i],titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]

        i1 = i1 + 1
        temp = titprivado_aux['data_expiracao'][i]
        while (pd.to_datetime(temp) > vencto):#(pd.to_datetime(titprivado_aux['data_expiracao'][i]))):
            qtde = qtde+1
            temp = titprivado_aux['data_expiracao'][i] - DateOffset(months=fatormensal*qtde, days=fatordiario*qtde)
            temp = datetime.date(temp.year,temp.month,temp.day)
            bmf_fluxo_titprivado_W.loc[i+i1] = [titprivado_aux['codigo_isin'][i],titprivado_aux['codigo_cetip'][i], titprivado_aux['tipo_ativo'][i], titprivado_aux['data_emissao'][i], titprivado_aux['data_expiracao'][i], titprivado_aux['valor_nominal'][i], titprivado_aux['taxa_juros'][i], titprivado_aux['indexador'][i], titprivado_aux['percentual_indexador'][i], titprivado_aux['cod_frequencia_juros'][i], titprivado_aux['data_primeiro_pagamento_juros'][i],titprivado_aux['id_bmf_numeraca'][i], data_bd,temp,titprivado_aux['flag'][i],titprivado_aux['id_papel'][i],titprivado_aux['dtoperacao'][i]]
            i1=i1+1

    bmf_fluxo_titprivado_W.to_excel(save_path_fluxo_W)

    #10 - Gera fluxo de papéis cod_frequencia_juros = OUTROS, NÃO APLICAVEIS, BULLET
    #cod_frequencia_juros
    #A - ANUAL; B - BI-ANUAL; M – MENSAL; N - NÃO APLICÁVEL; Q – TRIMESTRAL; S – SEMESTRAL; W - SEMANAL X – OUTROS
    del titprivado_aux

    titprivado_aux = titprivado[titprivado['cod_frequencia_juros'] != 'A'].copy()
    titprivado_aux = titprivado_aux[titprivado_aux['cod_frequencia_juros'] != 'B'].copy()
    titprivado_aux = titprivado_aux[titprivado_aux['cod_frequencia_juros'] != 'M'].copy()
    titprivado_aux = titprivado_aux[titprivado_aux['cod_frequencia_juros'] != 'Q'].copy()
    titprivado_aux = titprivado_aux[titprivado_aux['cod_frequencia_juros'] != 'S'].copy()
    titprivado_aux = titprivado_aux[titprivado_aux['cod_frequencia_juros'] != 'W'].copy()
    titprivado_aux = titprivado_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    bmf_fluxo_titprivado_aux = pd.DataFrame(columns=['codigo_isin',
                                                     'codigo_cetip',
                                                     'tipo_ativo',
                                                     'data_emissao',
                                                     'data_expiracao',
                                                     'valor_nominal',
                                                     'taxa_juros',
                                                     'indexador',
                                                     'percentual_indexador',
                                                     'cod_frequencia_juros',
                                                     'data_primeiro_pagamento_juros',
                                                     'id_bmf_numeraca',
                                                     'data_bd',
                                                     'dt_ref',
                                                     'flag',
                                                     'id_papel',
                                                     'dtoperacao'])

    bmf_fluxo_titprivado_aux['codigo_isin'] = titprivado_aux['codigo_isin']
    bmf_fluxo_titprivado_aux['codigo_cetip'] = titprivado_aux['codigo_cetip']
    bmf_fluxo_titprivado_aux['tipo_ativo'] = titprivado_aux['tipo_ativo']
    bmf_fluxo_titprivado_aux['data_emissao'] = titprivado_aux['data_emissao']
    bmf_fluxo_titprivado_aux['data_expiracao'] = titprivado_aux['data_expiracao']
    bmf_fluxo_titprivado_aux['valor_nominal'] = titprivado_aux['valor_nominal']
    bmf_fluxo_titprivado_aux['taxa_juros'] = titprivado_aux['taxa_juros']
    bmf_fluxo_titprivado_aux['indexador'] = titprivado_aux['indexador']
    bmf_fluxo_titprivado_aux['percentual_indexador'] = titprivado_aux['percentual_indexador']
    bmf_fluxo_titprivado_aux['cod_frequencia_juros'] = titprivado_aux['cod_frequencia_juros']
    bmf_fluxo_titprivado_aux['data_primeiro_pagamento_juros'] = titprivado_aux['data_expiracao']
    bmf_fluxo_titprivado_aux['id_bmf_numeraca'] = titprivado_aux['id_bmf_numeraca']
    bmf_fluxo_titprivado_aux['data_bd'] = data_bd
    bmf_fluxo_titprivado_aux['dt_ref'] = titprivado_aux['data_expiracao']
    bmf_fluxo_titprivado_aux['flag'] = titprivado_aux['flag']
    bmf_fluxo_titprivado_aux['id_papel'] = titprivado_aux['id_papel']
    bmf_fluxo_titprivado_aux['dtoperacao'] = titprivado_aux['dtoperacao']

    bmf_fluxo_titprivado = bmf_fluxo_titprivado_aux.copy()
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_A)
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_B)
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_S)
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_Q)
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_M)
    bmf_fluxo_titprivado = bmf_fluxo_titprivado.append(bmf_fluxo_titprivado_W)
    del bmf_fluxo_titprivado_aux

    bmf_fluxo_titprivado['dif'] = bmf_fluxo_titprivado['dt_ref'] - bmf_fluxo_titprivado['data_primeiro_pagamento_juros']
    bmf_fluxo_titprivado['dif'] = bmf_fluxo_titprivado['dif']/np.timedelta64(1, 'D')
    bmf_fluxo_titprivado['dif'] = bmf_fluxo_titprivado['dif'].astype(int)

    bmf_fluxo_titprivado = bmf_fluxo_titprivado[(bmf_fluxo_titprivado.dif>-6)]
    del bmf_fluxo_titprivado['dif']

    bmf_fluxo_titprivado['juros_tipo']='liquidate'
    bmf_fluxo_titprivado['percentual_juros']=1
    bmf_fluxo_titprivado['index_tipo']='liquidate'
    bmf_fluxo_titprivado['percentual_index']=1
    bmf_fluxo_titprivado['tipo']='vna'

    bmf_fluxo_titprivado['tipo_capitalizacao'] = 'Exponencial'
    bmf_fluxo_titprivado['dt_inicio_rentab'] = bmf_fluxo_titprivado['data_emissao']
    bmf_fluxo_titprivado['juros_cada'] = bmf_fluxo_titprivado['codigo_isin']
    bmf_fluxo_titprivado['juros_cada'] = None
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='A'] = 12
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='B'] = 24
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='S'] = 6
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='M'] = 1
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='Q'] = 3
    bmf_fluxo_titprivado['juros_cada'][bmf_fluxo_titprivado.cod_frequencia_juros=='W'] = 7
    bmf_fluxo_titprivado['juros_cada'][(bmf_fluxo_titprivado.cod_frequencia_juros!='A') & (bmf_fluxo_titprivado.cod_frequencia_juros!='B') & (bmf_fluxo_titprivado.cod_frequencia_juros!='S') & (bmf_fluxo_titprivado.cod_frequencia_juros!='M') & (bmf_fluxo_titprivado.cod_frequencia_juros!='Q') & (bmf_fluxo_titprivado.cod_frequencia_juros!='W')] = 0
    bmf_fluxo_titprivado['juros_unidade'] = bmf_fluxo_titprivado['codigo_isin']
    bmf_fluxo_titprivado['juros_unidade'] = None
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='A'] = 'MES'
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='B'] = 'MES'
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='S'] = 'MES'
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='M'] = 'MES'
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='Q'] = 'MES'
    bmf_fluxo_titprivado['juros_unidade'][bmf_fluxo_titprivado.cod_frequencia_juros=='W'] = 'DIA'
    bmf_fluxo_titprivado['juros_cada'][(bmf_fluxo_titprivado.cod_frequencia_juros!='A') & (bmf_fluxo_titprivado.cod_frequencia_juros!='B') & (bmf_fluxo_titprivado.cod_frequencia_juros!='S') & (bmf_fluxo_titprivado.cod_frequencia_juros!='M') & (bmf_fluxo_titprivado.cod_frequencia_juros!='Q') & (bmf_fluxo_titprivado.cod_frequencia_juros!='W')] = None
    bmf_fluxo_titprivado['juros_dc_du'] = 252
    bmf_fluxo_titprivado['indexador_dc_du'] = 252

    #Inverte a coluna codigo_isin pela id_papel
    bmf_fluxo_titprivado['codigo_isin_temp'] = bmf_fluxo_titprivado['codigo_isin']
    bmf_fluxo_titprivado['codigo_isin'] = bmf_fluxo_titprivado['id_papel']
    bmf_fluxo_titprivado['id_papel'] = bmf_fluxo_titprivado['codigo_isin_temp']

    #Premissa: toda vez que paga juros, paga principal
    bmf_fluxo_amort = bmf_fluxo_titprivado[bmf_fluxo_titprivado['juros_tipo']=='liquidate'].copy()
    quantidade = bmf_fluxo_amort[['codigo_isin', 'data_emissao','data_expiracao','juros_tipo']].groupby(['codigo_isin', 'data_emissao','data_expiracao']).agg(['count'])
    quantidade = quantidade.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
    qtde_amort = pd.DataFrame(columns=['codigo_isin', 'data_emissao','data_expiracao','qtde_amortizacao'])

    qtde_amort['codigo_isin']=quantidade['codigo_isin']
    qtde_amort['data_emissao']=quantidade['data_emissao']
    qtde_amort['data_expiracao']=quantidade['data_expiracao']
    qtde_amort['qtde_amortizacao']=quantidade['juros_tipo']
    qtde_amort['juros_tipo']='liquidate'
    qtde_amort['perc_amortizacao']=1/qtde_amort['qtde_amortizacao']*100
    del qtde_amort['qtde_amortizacao']

    bmf_fluxo_titprivado = bmf_fluxo_titprivado.merge(qtde_amort, on=['codigo_isin', 'data_emissao','data_expiracao','juros_tipo'], how='left')

    bmf_fluxo_titprivado['perc_amortizacao'][(bmf_fluxo_titprivado.tipo_ativo=='LH')|(bmf_fluxo_titprivado.tipo_ativo=='LFI')|(bmf_fluxo_titprivado.tipo_ativo=='LFN')|(bmf_fluxo_titprivado.tipo_ativo=='C')|(bmf_fluxo_titprivado.tipo_ativo=='CDB')|(bmf_fluxo_titprivado.tipo_ativo=='DP')] = 0
    bmf_fluxo_titprivado['perc_amortizacao'][(bmf_fluxo_titprivado.dt_ref==bmf_fluxo_titprivado.data_expiracao)&((bmf_fluxo_titprivado.tipo_ativo=='LH')|(bmf_fluxo_titprivado.tipo_ativo=='LFI')|(bmf_fluxo_titprivado.tipo_ativo=='LFN')|(bmf_fluxo_titprivado.tipo_ativo=='C')|(bmf_fluxo_titprivado.tipo_ativo=='CDB')|(bmf_fluxo_titprivado.tipo_ativo=='DP'))] = 1

    #Reinverte a coluna codigo_isin pela id_papel
    bmf_fluxo_titprivado['codigo_isin_temp'] = bmf_fluxo_titprivado['codigo_isin']
    bmf_fluxo_titprivado['codigo_isin'] = bmf_fluxo_titprivado['id_papel']
    bmf_fluxo_titprivado['id_papel'] = bmf_fluxo_titprivado['codigo_isin_temp']

    bmf_fluxo_titprivado = bmf_fluxo_titprivado.sort(['id_papel','dt_ref'],ascending=[True,True])

    del bmf_fluxo_titprivado['codigo_isin_temp']

    lista_final = bmf_fluxo_titprivado[['id_papel','codigo_isin']].copy()
    lista_final = lista_final.drop_duplicates(subset=['id_papel'])
    lista_final['marker'] = 1
    lista = lista_all.merge(lista_final,on=['id_papel','codigo_isin'],how='left')

    #Salvar no MySQL
    logger.info("Salvando base de dados - fluxo_titprivado")
    pd.io.sql.to_sql(bmf_fluxo_titprivado, name='fluxo_titprivado', con=connection, if_exists='append', flavor='mysql', index=0)

    #Fecha conexão
    connection.close()

    bmf_fluxo_titprivado.to_excel(save_path_fluxo_titprivado)


