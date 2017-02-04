def fluxo_debentures():

    import pandas as pd
    import numpy as np
    import datetime
    import pymysql as db
    import logging
    from pandas import ExcelWriter
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior

    logger = logging.getLogger(__name__)

    # Diretório de dependencias
    depend_path_carat = full_path_from_database('excels') + 'debenture_fluxo_manual.xlsx'

    # Diretório de save de planilhas
    xlsx_path_fluxo_debenture = full_path_from_database("get_output_quadro419") + 'controle_fluxo_debentures.xlsx'
    xlsx_path_anbima = full_path_from_database("get_output_quadro419") + 'fluxo_deb.xlsx'

    #Arquivo de controle
    writer = ExcelWriter(xlsx_path_fluxo_debenture)

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    #dtbase = ['2016','11','30']
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

    #---------------------------Importa dados crus

    #Conexão com Banco de Dados
    logger.info("Conectando no Banco de dados")
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv'
, use_unicode=True, charset="utf8")
    logger.info("Conexão com DB executada com sucesso")

    query = 'SELECT * FROM projeto_inv.debentures_caracteristicas WHERE data_de_vencimento >' + '"' + dtbase_concat + '";'
    data_aux = pd.read_sql(query, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    logger.info("Tratando dados")

    # Seleciona apenas papéis da data do relatório
    data_aux['dtrel'] = data_aux['id_papel'].str.split('_')
    data_aux['dtrel'] = data_aux['dtrel'].str[0]

    data_aux = data_aux[data_aux.dtrel == dtbase_concat].copy()
    data_aux = data_aux[data_aux.data_bd == max(data_aux.data_bd)].copy()

    data_aux = data_aux.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='').copy()

    aux_final = data_aux[['id_papel','isin','dtoperacao','amortizacao_carencia']].copy()
    aux_final = aux_final.rename(columns={'amortizacao_carencia':'data_do_primeiro_pagamento_amortizacao'})

    #---------------------------Data Quality

    # Tira os papéis com fluxo manual
    data_aux = data_aux[data_aux.flag!='FM']

    # Substitui nan por None
    data_aux=data_aux.where((pd.notnull(data_aux)), None)

    # Transforma '-' em None
    data_aux = data_aux.replace({'-': None}, regex = False)

    # Deleta Critério de Cálculo - Não padrão
    data_aux = data_aux[data_aux.criterio_de_calculo !='Não Padrão - SND']

    # Deleta observações com campo "Data de Vencimento" = "Indeterminado"
    data_aux = data_aux[data_aux.data_de_vencimento !='Indeterminado']

    # Altera papéis indexados a SEM ÍNDICE para PRE
    data_aux['indice'] = np.where(data_aux['indice']=='SEM-ÍNDICE','PRE',data_aux['indice'])

    # Preenche com a data de emissão onde a data de inicio de rentabilidade está vazio
    data_aux['data_do_inicio_da_rentabilidade'][data_aux.data_do_inicio_da_rentabilidade.isnull()] = data_aux['data_de_emissao'][data_aux.data_do_inicio_da_rentabilidade.isnull()]

    # Deleta observações com campo "Juros Carencia" = "-"
    data_aux[data_aux.juros_criterio_novo_carencia.isnull()].to_excel(writer,'juros_carencia_missing')
    data_aux = data_aux[data_aux.juros_criterio_novo_carencia.notnull()]

    # Deleta observações com campo "Valor Nominal de Emissão" em unidade diferente do real
    data_aux[data_aux.unidade_monetaria_1.isin(['NCz$','CR$','Cr$'])].to_excel(writer,'unidade_monetaria_del')
    data_aux = data_aux[data_aux.unidade_monetaria_1 != 'NCz$']
    data_aux = data_aux[data_aux.unidade_monetaria_1 != 'CR$']
    data_aux = data_aux[data_aux.unidade_monetaria_1 != 'Cr$']

    # Substitui o <tipo_de_amortizacao"
    data_aux = data_aux.replace(['Percentual fixo sobre o valor nominal atualizado em períodos uniformes'],['vna1'])
    data_aux = data_aux.replace(['Percentual fixo sobre o valor nominal atualizado em períodos não uniformes'],['vna2'])
    data_aux = data_aux.replace(['Percentual variável sobre o valor nominal atualizado em períodos não uniformes'],['vna3'])
    data_aux = data_aux.replace(['Percentual variável sobre o valor nominal atualizado em períodos uniformes'],['vna4'])
    data_aux = data_aux.replace(['Percentual fixo sobre o valor nominal de emissão em períodos uniformes'],['vne1'])
    data_aux = data_aux.replace(['Percentual fixo sobre o valor nominal de emissão em períodos não uniformes'],['vne2'])
    data_aux = data_aux.replace(['Percentual variável sobre o valor nominal de emissão em períodos não uniformes'],['vne3'])
    data_aux = data_aux.replace(['Percentual variável sobre o valor nominal de emissão em períodos uniformes'],['vne4'])

    # Deleta Amortização variável/períodos não uniformes - PAPÉIS COM  FLUXO MANUAL
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vne2']
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vne3']
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vne4']
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vna2']
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vna3']
    data_aux = data_aux[data_aux.tipo_de_amortizacao !='vna4']
    data_aux[data_aux.tipo_de_amortizacao.isin(['vne2','vne3','vne4','vna2','vna3','vna4'])].to_excel(writer,'tipo_de_amortizacao_del')

    # Quando tem amortização, diz sobre o que é pago
    data_aux['tipo_de_amortizacao'] = data_aux['tipo_de_amortizacao'].replace(['vna1'],['vna'])
    data_aux['tipo_de_amortizacao'] = data_aux['tipo_de_amortizacao'].replace(['vne1'],['vne'])

    # Correção de nome dos indexadores
    data_aux['indice'] = data_aux['indice'].replace(['IGP-M'],['IGP'])
    data_aux['indice'] = data_aux['indice'].replace(['DI'],['DI1'])
    data_aux['indice'] = data_aux['indice'].replace(['PRÉ'],['PRE'])

    # Reseta os índices do dataframe
    data_aux = data_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # Converte string em formato de data
    data_aux['data_de_emissao'] = pd.to_datetime(data_aux['data_de_emissao'])
    data_aux['data_de_vencimento'] = pd.to_datetime(data_aux['data_de_vencimento'])
    data_aux['data_do_inicio_da_rentabilidade'] = pd.to_datetime(data_aux['data_do_inicio_da_rentabilidade'])
    data_aux['amortizacao_carencia'] = pd.to_datetime(data_aux['amortizacao_carencia'])
    data_aux['juros_criterio_novo_carencia'] = pd.to_datetime(data_aux['juros_criterio_novo_carencia'])

    # Preenche buracos de padrão 252/360 para atualização de indice - 252 Default
    data_aux['criterio_para_indice'] = data_aux['criterio_para_indice'].where(data_aux['criterio_para_indice'] != 'DC', 360)
    data_aux['criterio_para_indice'] = data_aux['criterio_para_indice'].where(data_aux['criterio_para_indice'] != 'DU', 252)
    data_aux['criterio_para_indice'][data_aux['criterio_para_indice'].isnull()] = 252

    # Preenche buracos de padrão 252/360 para acumulação de juros - 252 Default
    data_aux['juros_criterio_novo_prazo'] = data_aux['juros_criterio_novo_prazo'].where(data_aux['juros_criterio_novo_prazo'] != '0,', '252')
    data_aux['juros_criterio_novo_prazo'] = data_aux['juros_criterio_novo_prazo'].where(data_aux['juros_criterio_novo_prazo'] != '0.', '252')
    data_aux['juros_criterio_novo_prazo'][(data_aux.juros_criterio_novo_prazo!='252') & (data_aux.juros_criterio_novo_prazo!='360')] = '252'

    # Passa valor_nominal_na_emissao de string para float
    data_aux['valor_nominal_na_emissao'] = data_aux['valor_nominal_na_emissao'].astype(float)

    #Passa taxa de juros de string para float
    data_aux['juros_criterio_novo_taxa'] = data_aux['juros_criterio_novo_taxa'].str.replace(',', '.')
    #Na base de dados a taxa de juros da Betty está como varchar
    #data_aux['juros_criterio_novo_taxa'] = data_aux['juros_criterio_novo_taxa'].astype(float)

    # Passa %rentabilidade de string para float
    data_aux['percentual_multiplicador_rentabilidade'] = data_aux['percentual_multiplicador_rentabilidade'].str.replace(',', '.')
    data_aux['percentual_multiplicador_rentabilidade'] = data_aux['percentual_multiplicador_rentabilidade'].astype(float)

    # Passa juros_criiterio_novo_cada de string para float
    data_aux['juros_criterio_novo_cada'] = np.where(data_aux['juros_criterio_novo_cada'].isnull(),'0.',data_aux['juros_criterio_novo_cada'])
    data_aux['juros_criterio_novo_cada'] = np.where(data_aux['juros_criterio_novo_cada']=='None','0.',data_aux['juros_criterio_novo_cada'])
    data_aux['juros_criterio_novo_cada'] = data_aux['juros_criterio_novo_cada'].astype(str)
    data_aux['juros_criterio_novo_cada'] = data_aux['juros_criterio_novo_cada'].str.replace(',', '.')
    data_aux['juros_criterio_novo_cada'] = data_aux['juros_criterio_novo_cada'].where(data_aux['juros_criterio_novo_cada'] != '0.', 0)
    data_aux['juros_criterio_novo_cada'] = data_aux['juros_criterio_novo_cada'].astype(int)

    # Passa amortização_cada de string para float
    data_aux['amortizacao_cada'] = data_aux['amortizacao_cada'][data_aux.amortizacao_cada.notnull()].astype(int)

    # Passa taxa de amortizacao de string para float e preenche os None
    data_aux['amortizacao_taxa'] = 0

    dtbase_mtm = dtbase_concat
    ano = str(dtbase_mtm)[0:4]
    mes = str(dtbase_mtm)[4:6]
    dia = str(dtbase_mtm)[6:8]
    data_base_mtm = datetime.date(int(ano),int(mes),int(dia))

    #Tira papéis já vencidos
    data_aux = data_aux[data_aux.data_de_vencimento>=data_base_mtm]

    #Reseta os índices do dataframe
    data_aux = data_aux.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    #---------------------------TABELA INFO
    fx_r0 = pd.DataFrame(columns=['id_andima_debentures',
                                  'codigo_isin',
                                  'id_papel',
                                  'flag',
                                  'data_emissao',
                                  'data_expiracao',
                                  'dt_inicio_rentab',
                                  'data_primeiro_pagamento_juros',
                                  'data_do_primeiro_pagamento_amortizacao',
                                  'valor_nominal',
                                  'tipo_capitalizacao',
                                  'indexador',
                                  'indexador_dc_du',
                                  'percentual_indexador',
                                  'juros_cada',
                                  'juros_unidade',
                                  'juros_dc_du',
                                  'taxa_juros',
                                  'tipo_amortizacao',
                                  'amortizacao_cada',
                                  'amortizacao_unidade',
                                  'taxa_amortizacao'
                                  ])

    fx_r0['id_andima_debentures'] = data_aux['id_debentures_caracteristicas'].copy()
    fx_r0['codigo_isin'] = data_aux['isin'].copy()
    fx_r0['id_papel'] = data_aux['id_papel'].copy()
    fx_r0['flag'] = data_aux['flag'].copy()
    fx_r0['data_emissao'] = data_aux['data_de_emissao'].copy()
    fx_r0['data_expiracao'] = data_aux['data_de_vencimento'].copy()
    fx_r0['dtoperacao'] = data_aux['dtoperacao'].copy()
    fx_r0['dt_inicio_rentab'] = data_aux['data_do_inicio_da_rentabilidade'].copy()
    fx_r0['data_primeiro_pagamento_juros'] = data_aux['juros_criterio_novo_carencia'].copy()
    fx_r0['data_do_primeiro_pagamento_amortizacao'] = data_aux['amortizacao_carencia'].copy()
    fx_r0['valor_nominal'] = data_aux['valor_nominal_na_emissao'].copy()
    fx_r0['tipo_capitalizacao'] = data_aux['juros_criterio_novo_tipo'].copy()
    fx_r0['indexador'] = data_aux['indice'].copy()
    fx_r0['indexador_dc_du'] = data_aux['criterio_para_indice'].copy()
    fx_r0['percentual_indexador'] = data_aux['percentual_multiplicador_rentabilidade'].copy()
    fx_r0['juros_cada'] = data_aux['juros_criterio_novo_cada'].copy()
    fx_r0['juros_unidade'] = data_aux['juros_criterio_novo_unidade'].copy()
    fx_r0['juros_dc_du'] = data_aux['juros_criterio_novo_prazo'].copy()
    fx_r0['taxa_juros'] = data_aux['juros_criterio_novo_taxa'].copy()
    fx_r0['tipo_amortizacao'] = data_aux['tipo_de_amortizacao'].copy()
    fx_r0['amortizacao_cada'] = data_aux['amortizacao_cada'].copy()
    fx_r0['amortizacao_unidade'] = data_aux['amortizacao_unidade'].copy()
    fx_r0['taxa_amortizacao'] = data_aux['amortizacao_taxa'].copy()

    #---------------------------INICIALIZA TABELA FLUXO
    anbima_fluxo_debenture = pd.DataFrame(columns=['id_papel',
                                                   'codigo_isin',
                                                   'flag',
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
                                                   'juros_tipo',
                                                   'percentual_juros',
                                                   'index_tipo',
                                                   'percentual_index',
                                                   'amt_tipo',
                                                   'tipo',
                                                   'id_andima_debentures',
                                                   'perc_amortizacao',
                                                   'tipo_capitalizacao',
                                                   'dt_inicio_rentab',
                                                   'juros_cada',
                                                   'juros_unidade',
                                                   'juros_dc_du',
                                                   'indexador_dc_du',
                                                   'flag_tipo'
                                                   ])
    #Cria a data da carga na base de dados
    horario_bd = datetime.datetime.now()

    #---------------------------PREENCHE BULLETS
    #Cria vetor de indexadores
    idx = fx_r0[fx_r0.data_expiracao == fx_r0.data_primeiro_pagamento_juros].index.tolist()

    if len(idx)!=0:

        anbima_fluxo_debenture['codigo_isin'] = fx_r0['codigo_isin'][idx].copy()
        anbima_fluxo_debenture['id_papel'] = fx_r0['id_papel'][idx].copy()
        anbima_fluxo_debenture['flag'] = fx_r0['flag'][idx].copy()
        anbima_fluxo_debenture['data_emissao'] = fx_r0['data_emissao'][idx].copy()
        anbima_fluxo_debenture['data_expiracao'] = fx_r0['data_expiracao'][idx].copy()
        anbima_fluxo_debenture['valor_nominal'] = fx_r0['valor_nominal'][idx].copy()
        anbima_fluxo_debenture['taxa_juros'] = fx_r0['taxa_juros'][idx].copy()
        anbima_fluxo_debenture['indexador'] = fx_r0['indexador'][idx].copy()
        anbima_fluxo_debenture['percentual_indexador'] = fx_r0['percentual_indexador'][idx].copy()
        anbima_fluxo_debenture['data_primeiro_pagamento_juros'] = fx_r0['data_expiracao'][idx].copy()
        anbima_fluxo_debenture['dt_ref'] = fx_r0['data_expiracao'][idx].copy()
        anbima_fluxo_debenture['juros_tipo'] = 'liquidate'
        anbima_fluxo_debenture['percentual_juros'] = 1
        anbima_fluxo_debenture['index_tipo'] = 'liquidate'
        anbima_fluxo_debenture['percentual_index'] = 1
        anbima_fluxo_debenture['amt_tipo'] = 1
        anbima_fluxo_debenture['tipo'] = fx_r0['tipo_amortizacao'][idx].copy()
        anbima_fluxo_debenture['tipo_capitalizacao'] = fx_r0['tipo_capitalizacao'][idx].copy()
        anbima_fluxo_debenture['dt_inicio_rentab'] = fx_r0['dt_inicio_rentab'][idx].copy()
        anbima_fluxo_debenture['juros_unidade'] = fx_r0['juros_unidade'][idx].copy()
        anbima_fluxo_debenture['juros_cada'] = fx_r0['juros_cada'][idx].copy()
        anbima_fluxo_debenture['juros_dc_du'] = fx_r0['juros_dc_du'][idx].copy()
        anbima_fluxo_debenture['indexador_dc_du'] = fx_r0['indexador_dc_du'][idx].copy()
        anbima_fluxo_debenture['id_andima_debentures'] = data_aux['id_debentures_caracteristicas'].copy()
        anbima_fluxo_debenture['perc_amortizacao'] = 100.0
        anbima_fluxo_debenture['data_bd'] = horario_bd
        anbima_fluxo_debenture['flag_tipo'] = 'bullet'

        #Reseta os índices do dataframe tabela fluxo
        anbima_fluxo_debenture = anbima_fluxo_debenture.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

        #Deleta papéis preenchidos
        fx_r0.drop(fx_r0.index[idx], inplace = True)

        #Reseta os índices do dataframe tabela info
        fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')


    #---------------------------PREENCHE PAPÉIS MENSAIS SEM AMORTIZAÇÃO
    # Cria lista de indexadores
    idx = fx_r0[(fx_r0.juros_unidade == 'MES') & (
    fx_r0.data_do_primeiro_pagamento_amortizacao == fx_r0.data_expiracao)].index.tolist()

    # Preenche papéis sem amortização
    afd_count = len(anbima_fluxo_debenture)
    afd_idx = 0
    for i in idx:
        # Preenche a primeira linha do novo papel
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                 fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                 fx_r0['flag'][i],  # 'flag'
                                                 None,  # 'codigo_cetip'
                                                 'DBS',  # 'tipo_ativo'
                                                 fx_r0['data_emissao'][i],  # 'data_emissao'
                                                 fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                 fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                 fx_r0['taxa_juros'][i],  # 'taxa_juros'
                                                 fx_r0['indexador'][i],  # 'indexador'
                                                 fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                 None,  # 'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],
                                                 # 'data_primeiro_pagamento_juros'
                                                 None,  # 'id_bmf_numeraca'
                                                 horario_bd,  # 'data_bd'
                                                 #                                             None,
                                                 fx_r0['data_expiracao'][i],  # 'dt_ref'
                                                 'liquidate',  # 'juros_tipo'
                                                 1,  # 'percentual_juros'
                                                 'liquidate',  # 'index_tipo'
                                                 1,  # 'percentual_index'
                                                 0,  # 'amt'
                                                 fx_r0['tipo_amortizacao'][i],  # 'tipo'
                                                 fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                 fx_r0['taxa_amortizacao'][i],  # 'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],  # 'juros_cada'
                                                 fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                 '01'  # 'flag_tipo'
                                                 ]

        ofset = 1
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= anbima_fluxo_debenture['data_primeiro_pagamento_juros'][
            afd_count]:
            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                     fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                     fx_r0['flag'][i],  # 'flag'
                                                     None,  # 'codigo_cetip'
                                                     'DBS',  # 'tipo_ativo'
                                                     fx_r0['data_emissao'][i],  # 'data_emissao'
                                                     fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                     fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                     fx_r0['taxa_juros'][i],  # 'taxa_juros'
                                                     fx_r0['indexador'][i],  # 'indexador'
                                                     fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                     None,  # 'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],
                                                     # 'data_primeiro_pagamento_juros'
                                                     None,  # 'id_bmf_numeraca'
                                                     horario_bd,  # 'data_bd'
                                                     #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(
                                                         months=int(fx_r0['juros_cada'][i]) * ofset),  # 'dt_ref'
                                                     'liquidate',  # 'juros_tipo'
                                                     1,  # 'percentual_juros'
                                                     'liquidate',  # 'index_tipo'
                                                     1,  # 'percentual_index'
                                                     0,  # 'amt'
                                                     fx_r0['tipo_amortizacao'][i],  # 'tipo'
                                                     fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                     fx_r0['taxa_amortizacao'][i],  # 'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],  # 'juros_cada'
                                                     fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                     '02'  # 'flag_tipo'
                                                     ]

            ofset = ofset + 1
        afd_idx = afd_idx + 1
    # Deleta papéis preenchidos
    fx_r0.drop(fx_r0.index[idx], inplace=True)

    # Reseta os índices do dataframe tabela info
    fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')


    #---------------------------PREENCHE PAPÉIS MENSAIS COM AMORTIZAÇÃO PAGANDO JUNTO COM JUROS
    #Cria lista de indexadores
    idx = fx_r0[(fx_r0.juros_unidade == 'MES') & (fx_r0.data_do_primeiro_pagamento_amortizacao == fx_r0.data_primeiro_pagamento_juros) & (fx_r0.amortizacao_cada == fx_r0.juros_cada)].index.tolist()

    #Preenche papéis sem amortização
    afd_count = len(anbima_fluxo_debenture)
    afd_idx = 0
    for i in idx:
        #Preenche a primeira linha do novo papel
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                 fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                 fx_r0['flag'][i],                                      #'flag'
                                                 None,                                                      #'codigo_cetip'
                                                 'DBS',                                                     #'tipo_ativo'
                                                 fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                 fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                 fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                 fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                 fx_r0['indexador'][i],                                     #'indexador'
                                                 fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                 None,                                                      #'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                 None,                                                      #'id_bmf_numeraca'
                                                 horario_bd,                                                #'data_bd'
    #                                             None,
                                                 fx_r0['data_expiracao'][i],                                #'dt_ref'
                                                 'liquidate',                                               #'juros_tipo'
                                                 1,                                                         #'percentual_juros'
                                                 'liquidate',                                               #'index_tipo'
                                                 1,                                                         #'percentual_index'
                                                 1,                                                         #'amt'
                                                 fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                 fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                 fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                 fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],                               #'indexador_dc_du'
                                                 '1'                                                        #'flag_tipo'
                                                 ]

        ofset = 1
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= anbima_fluxo_debenture['data_primeiro_pagamento_juros'][afd_count]:

            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                     fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                     fx_r0['flag'][i],                                      #'flag'
                                                     None,                                                      #'codigo_cetip'
                                                     'DBS',                                                     #'tipo_ativo'
                                                     fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                     fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                     fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                     fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                     fx_r0['indexador'][i],                                     #'indexador'
                                                     fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                     None,                                                      #'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                     None,                                                      #'id_bmf_numeraca'
                                                     horario_bd,                                                #'data_bd'
    #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(months = int(fx_r0['juros_cada'][i])*ofset), #'dt_ref'
                                                     'liquidate',                                               #'juros_tipo'
                                                     1,                                                         #'percentual_juros'
                                                     'liquidate',                                               #'index_tipo'
                                                     1,                                                         #'percentual_index'
                                                     1,                                                         #'amt'
                                                     fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                     fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                     fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                     fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                     '2'                                                       #'flag_tipo'
                                                     ]

            ofset = ofset + 1
        afd_idx = afd_idx + 1
    #Deleta papéis preenchidos
    fx_r0.drop(fx_r0.index[idx], inplace = True)

    #Reseta os índices do dataframe tabela info
    fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    #---------------------------PREENCHE PAPÉIS MENSAIS COM AMORTIZAÇÃO PAGANDO SEPARADO DOS JUROS
    afd_count = len(anbima_fluxo_debenture)

    #Cria lista de indexadores
    idx = fx_r0[(fx_r0.juros_unidade == 'MES') & (fx_r0.amortizacao_unidade == 'MES') & (((fx_r0.juros_cada != fx_r0.amortizacao_cada)&(fx_r0.data_primeiro_pagamento_juros==fx_r0.data_do_primeiro_pagamento_amortizacao))|((fx_r0.juros_cada == fx_r0.amortizacao_cada)&(fx_r0.data_primeiro_pagamento_juros!=fx_r0.data_do_primeiro_pagamento_amortizacao))|((fx_r0.juros_cada != fx_r0.amortizacao_cada)&(fx_r0.data_primeiro_pagamento_juros!=fx_r0.data_do_primeiro_pagamento_amortizacao)))].index.tolist()

    #Preenche papéis sem amortização
    afd_count = afd_count + 1
    #afd_idx = len(anbima_fluxo_debenture) - 1
    for i in idx:
        #Preenche a primeira linha do novo papel - JUROS
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                 fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                 fx_r0['flag'][i],                                      #'flag'
                                                 None,                                                      #'codigo_cetip'
                                                 'DBS',                                                     #'tipo_ativo'
                                                 fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                 fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                 fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                 fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                 fx_r0['indexador'][i],                                     #'indexador'
                                                 fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                 None,                                                      #'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                 None,                                                      #'id_bmf_numeraca'
                                                 horario_bd,                                                #'data_bd'
    #                                             None,
                                                 fx_r0['data_expiracao'][i],                                #'dt_ref'
                                                 'liquidate',                                               #'juros_tipo'
                                                 1,                                                         #'percentual_juros'
                                                 'liquidate',                                               #'index_tipo'
                                                 1,                                                         #'percentual_index'
                                                 0,                                                         #'amt'
                                                 fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                 fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                 0.0,                                                       #'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                 fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                 '3'                                                       #'flag_tipo'
                                                 ]

        ofset = 1
        #Preenche as outras linhas de cada papel - JUROS
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= anbima_fluxo_debenture['data_primeiro_pagamento_juros'][afd_count]:

            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                     fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                     fx_r0['flag'][i],                                      #'flag'
                                                     None,                                                      #'codigo_cetip'
                                                     'DBS',                                                     #'tipo_ativo'
                                                     fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                     fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                     fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                     fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                     fx_r0['indexador'][i],                                     #'indexador'
                                                     fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                     None,                                                      #'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                     None,                                                      #'id_bmf_numeraca'
                                                     horario_bd,                                                #'data_bd'
    #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(months = int(fx_r0['juros_cada'][i])*ofset), #'dt_ref'
                                                     'liquidate',                                               #'juros_tipo'
                                                     1,                                                         #'percentual_juros'
                                                     'liquidate',                                               #'index_tipo'
                                                     1,                                                         #'percentual_index'
                                                     0,                                                         #'amt'
                                                     fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                     fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                     0.0,                                                       #'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                     fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                     '4'                                                        #'flag_tipo'
                                                     ]

            ofset = ofset + 1
        #Preenche o primeiro pagamento - AMORTIZAÇÃO
        afd_count = afd_count + 1
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                 fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                 fx_r0['flag'][i],                                          #'flag'
                                                 None,                                                      #'codigo_cetip'
                                                 'DBS',                                                     #'tipo_ativo'
                                                 fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                 fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                 fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                 0.0,                                                       #'taxa_juros'
                                                 fx_r0['indexador'][i],                                     #'indexador'
                                                 fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                 None,                                                      #'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                 None,                                                      #'id_bmf_numeraca'
                                                 horario_bd,                                                #'data_bd'
    #                                             None,
                                                 fx_r0['data_expiracao'][i],                                #'dt_ref'
                                                 'accrue',                                                  #'juros_tipo'
                                                 1,                                                         #'percentual_juros'
                                                 'accrue',                                                  #'index_tipo'
                                                 1,                                                         #'percentual_index'
                                                 1,                                                         #'amt'
                                                 fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                 fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                 fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                 fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],                               #'indexador_dc_du'
                                                 '5'                                                        #'flag_tipo'
                                                 ]

        ofset = 1
        #Preenche as outras linhas de cada papel - AMORTIZAÇÃO
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= fx_r0['data_do_primeiro_pagamento_amortizacao'][i]:
            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                     fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                     fx_r0['flag'][i],                                      #'flag'
                                                     None,                                                      #'codigo_cetip'
                                                     'DBS',                                                     #'tipo_ativo'
                                                     fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                     fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                     fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                     0.0,                                                       #'taxa_juros'
                                                     fx_r0['indexador'][i],                                     #'indexador'
                                                     fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                     None,                                                      #'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                     None,                                                      #'id_bmf_numeraca'
                                                     horario_bd,                                                #'data_bd'
    #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(months = int(fx_r0['amortizacao_cada'][i])*ofset), #'dt_ref'
                                                     'accrue',                                                  #'juros_tipo'
                                                     1,                                                         #'percentual_juros'
                                                     'accrue',                                                  #'index_tipo'
                                                     1,                                                         #'percentual_index'
                                                     1,                                                         #'amt'
                                                     fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                     fx_r0['id_andima_debentures'][i],                          #'id_andima_debentures'
                                                     fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                     fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                     '6'                                                       #'flag_tipo'
                                                     ]

            ofset = ofset + 1
        afd_idx = afd_idx + 1

    #Deleta papéis preenchidos
    fx_r0.drop(fx_r0.index[idx], inplace = True)

    #Reseta os índices do dataframe tabela info
    fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    #---------------------------PREENCHE PAPÉIS DIÁRIOS SEM AMORTIZAÇÃO
    afd_count = len(anbima_fluxo_debenture)

    #Cria lista de indexadores
    idx = fx_r0[(fx_r0.juros_unidade == 'DIA') & (fx_r0.data_do_primeiro_pagamento_amortizacao==fx_r0.data_expiracao)].index.tolist()

    afd_idx=0
    #Preenche papéis sem amortização
    afd_count = len(anbima_fluxo_debenture)
    for i in idx:
    #    print('DIARIO')
        #Preenche a primeira linha do novo papel
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                 fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                 fx_r0['flag'][i],                                      #'flag'
                                                 None,                                                      #'codigo_cetip'
                                                 'DBS',                                                     #'tipo_ativo'
                                                 fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                 fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                 fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                 fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                 fx_r0['indexador'][i],                                     #'indexador'
                                                 fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                 None,                                                      #'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                 None,                                                      #'id_bmf_numeraca'
                                                 horario_bd,                                                #'data_bd'
    #                                             None,
                                                 fx_r0['data_expiracao'][i],                                #'dt_ref'
                                                 'liquidate',                                               #'juros_tipo'
                                                 1,                                                         #'percentual_juros'
                                                 'liquidate',                                               #'index_tipo'
                                                 1,                                                         #'percentual_index'
                                                 0,                                                         #'amt'
                                                 fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                 fx_r0['id_andima_debentures'][i],                                                   #'id_andima_debentures'
                                                 fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                 fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                 '7'                                                       #'flag_tipo'
                                                 ]



        #Cada_ofst = DateOffset(months = int(fx_r0['juros_cada'][i]))
        ofset = 1
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= anbima_fluxo_debenture['data_primeiro_pagamento_juros'][afd_count]:

            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],                                      #'id_papel',
                                                     fx_r0['codigo_isin'][i],                                   #'codigo_isin'
                                                     fx_r0['flag'][i],                                      #'flag'
                                                     None,                                                      #'codigo_cetip'
                                                     'DBS',                                                     #'tipo_ativo'
                                                     fx_r0['data_emissao'][i],                                  #'data_emissao'
                                                     fx_r0['data_expiracao'][i],                                #'data_expiracao'
                                                     fx_r0['valor_nominal'][i],                                 #'valor_nominal'
                                                     fx_r0['taxa_juros'][i],                                    #'taxa_juros'
                                                     fx_r0['indexador'][i],                                     #'indexador'
                                                     fx_r0['percentual_indexador'][i],                          #'percentual_indexador'
                                                     None,                                                      #'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],                 #'data_primeiro_pagamento_juros'
                                                     None,                                                      #'id_bmf_numeraca'
                                                     horario_bd,                                                #'data_bd'
    #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(months = int(fx_r0['juros_cada'][i])*ofset), #'dt_ref'
                                                     'liquidate',                                               #'juros_tipo'
                                                     1,                                                         #'percentual_juros'
                                                     'liquidate',                                               #'index_tipo'
                                                     1,                                                         #'percentual_index'
                                                     0,                                                         #'amt'
                                                     fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                     fx_r0['id_andima_debentures'][i],                                                   #'id_andima_debentures'
                                                     fx_r0['taxa_amortizacao'][i],                              #'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],                            #'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],                              #'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],                                    #'juros_cada'
                                                     fx_r0['juros_unidade'][i],                                 #'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],                                   #'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],                                #'indexador_dc_du'
                                                     '8'                                                       #'flag_tipo'
                                                     ]

            ofset = ofset + 1
        afd_idx = afd_idx + 1
    #Deleta papéis preenchidos
    fx_r0.drop(fx_r0.index[idx], inplace = True)

    #Reseta os índices do dataframe tabela info
    fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # ---------------------------PREENCHE PAPÉIS DIARIOS COM AMORTIZAÇÃO MENSAL
    afd_count = len(anbima_fluxo_debenture)

    # Cria lista de indexadores
    idx = fx_r0[(fx_r0.juros_unidade == 'DIA') & (fx_r0.amortizacao_unidade == 'MES')].index.tolist()

    # Preenche papéis sem amortização
    afd_count = afd_count + 1
    # afd_idx = len(anbima_fluxo_debenture) - 1
    for i in idx:
        #   print('DIARIO_AMORTIZACAO')
        # Preenche a primeira linha do novo papel - JUROS
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                 fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                 fx_r0['flag'][i],  # 'flag'
                                                 None,  # 'codigo_cetip'
                                                 'DBS',  # 'tipo_ativo'
                                                 fx_r0['data_emissao'][i],  # 'data_emissao'
                                                 fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                 fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                 fx_r0['taxa_juros'][i],  # 'taxa_juros'
                                                 fx_r0['indexador'][i],  # 'indexador'
                                                 fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                 None,  # 'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],
                                                 # 'data_primeiro_pagamento_juros'
                                                 None,  # 'id_bmf_numeraca'
                                                 horario_bd,  # 'data_bd'
                                                 #                                             None,
                                                 fx_r0['data_expiracao'][i],  # 'dt_ref'
                                                 'liquidate',  # 'juros_tipo'
                                                 1,  # 'percentual_juros'
                                                 'liquidate',  # 'index_tipo'
                                                 1,  # 'percentual_index'
                                                 0,  # 'amt'
                                                 fx_r0['tipo_amortizacao'][i],  # 'tipo'
                                                 fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                 0.0,  # 'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],  # 'juros_cada'
                                                 fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                 '9'  # 'flag_tipo'
                                                 ]

        ofset = 1
        # Preenche as outras linhas de cada papel - JUROS
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= anbima_fluxo_debenture['data_primeiro_pagamento_juros'][
            afd_count]:
            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                     fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                     fx_r0['flag'][i],  # 'flag'
                                                     None,  # 'codigo_cetip'
                                                     'DBS',  # 'tipo_ativo'
                                                     fx_r0['data_emissao'][i],  # 'data_emissao'
                                                     fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                     fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                     fx_r0['taxa_juros'][i],  # 'taxa_juros'
                                                     fx_r0['indexador'][i],  # 'indexador'
                                                     fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                     None,  # 'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],
                                                     # 'data_primeiro_pagamento_juros'
                                                     None,  # 'id_bmf_numeraca'
                                                     horario_bd,  # 'data_bd'
                                                     #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(
                                                         months=int(fx_r0['juros_cada'][i]) * ofset),  # 'dt_ref'
                                                     'liquidate',  # 'juros_tipo'
                                                     1,  # 'percentual_juros'
                                                     'liquidate',  # 'index_tipo'
                                                     1,  # 'percentual_index'
                                                     0,  # 'amt'
                                                     fx_r0['tipo_amortizacao'][i],  # 'tipo'
                                                     fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                     0.0,  # 'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],  # 'juros_cada'
                                                     fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                     '10'  # 'flag_tipo'
                                                     ]

            ofset = ofset + 1
        # Preenche o primeiro pagamento - AMORTIZAÇÃO
        afd_count = afd_count + 1
        anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                 fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                 fx_r0['flag'][i],  # 'flag'
                                                 None,  # 'codigo_cetip'
                                                 'DBS',  # 'tipo_ativo'
                                                 fx_r0['data_emissao'][i],  # 'data_emissao'
                                                 fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                 fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                 0.0,  # 'taxa_juros'
                                                 fx_r0['indexador'][i],  # 'indexador'
                                                 fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                 None,  # 'cod_frequencia_juros'
                                                 fx_r0['data_primeiro_pagamento_juros'][i],
                                                 # 'data_primeiro_pagamento_juros'
                                                 None,  # 'id_bmf_numeraca'
                                                 horario_bd,  # 'data_bd'
                                                 #                                             None,
                                                 fx_r0['data_expiracao'][i],  # 'dt_ref'
                                                 'accrue',  # 'juros_tipo'
                                                 1,  # 'percentual_juros'
                                                 'accrue',  # 'index_tipo'
                                                 1,  # 'percentual_index'
                                                 1,
                                                 # 'amt'                                             fx_r0['tipo_amortizacao'][i],                              #'tipo'
                                                 fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                 fx_r0['taxa_amortizacao'][i],  # 'perc_amortizacao'
                                                 fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                 fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                 fx_r0['juros_cada'][i],  # 'juros_cada'
                                                 fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                 fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                 fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                 '11'  # 'flag_tipo'
                                                 ]

        ofset = 1
        # Preenche as outras linhas de cada papel - AMORTIZAÇÃO
        while anbima_fluxo_debenture['dt_ref'][afd_count] >= fx_r0['data_do_primeiro_pagamento_amortizacao'][i]:
            afd_count = afd_count + 1
            anbima_fluxo_debenture.loc[afd_count] = [fx_r0['id_papel'][i],  # 'id_papel',
                                                     fx_r0['codigo_isin'][i],  # 'codigo_isin'
                                                     fx_r0['flag'][i],  # 'flag'
                                                     None,  # 'codigo_cetip'
                                                     'DBS',  # 'tipo_ativo'
                                                     fx_r0['data_emissao'][i],  # 'data_emissao'
                                                     fx_r0['data_expiracao'][i],  # 'data_expiracao'
                                                     fx_r0['valor_nominal'][i],  # 'valor_nominal'
                                                     0.0,  # 'taxa_juros'
                                                     fx_r0['indexador'][i],  # 'indexador'
                                                     fx_r0['percentual_indexador'][i],  # 'percentual_indexador'
                                                     None,  # 'cod_frequencia_juros'
                                                     fx_r0['data_primeiro_pagamento_juros'][i],
                                                     # 'data_primeiro_pagamento_juros'
                                                     None,  # 'id_bmf_numeraca'
                                                     horario_bd,  # 'data_bd'
                                                     #                                                 None,
                                                     fx_r0['data_expiracao'][i] - pd.DateOffset(
                                                         months=int(fx_r0['amortizacao_cada'][i]) * ofset),  # 'dt_ref'
                                                     'accrue',  # 'juros_tipo'
                                                     1,  # 'percentual_juros'
                                                     'accrue',  # 'index_tipo'
                                                     1,  # 'percentual_index'
                                                     1,  # 'amt'
                                                     fx_r0['tipo_amortizacao'][i],  # 'tipo'
                                                     fx_r0['id_andima_debentures'][i],  # 'id_andima_debentures'
                                                     fx_r0['taxa_amortizacao'][i],  # 'perc_amortizacao'
                                                     fx_r0['tipo_capitalizacao'][i],  # 'tipo_capitalizacao'
                                                     fx_r0['dt_inicio_rentab'][i],  # 'dt_inicio_rentab'
                                                     fx_r0['juros_cada'][i],  # 'juros_cada'
                                                     fx_r0['juros_unidade'][i],  # 'juros_unidade'
                                                     fx_r0['juros_dc_du'][i],  # 'juros_dc_du'
                                                     fx_r0['indexador_dc_du'][i],  # 'indexador_dc_du'
                                                     '12'  # 'flag_tipo'
                                                     ]

            ofset = ofset + 1
        afd_idx = afd_idx + 1

    fx_r0.drop(fx_r0.index[idx], inplace=True)

    # Reseta os índices do dataframe tabela info
    fx_r0 = fx_r0.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    # Ultimas correções bd
    #Ajustes
    anbima_fluxo_debenture = anbima_fluxo_debenture.where((pd.notnull(anbima_fluxo_debenture)), None)
    anbima_fluxo_debenture['tipo_ativo'] = anbima_fluxo_debenture['tipo_ativo'].fillna('DBS')
    anbima_fluxo_debenture['perc_amortizacao'] = anbima_fluxo_debenture['perc_amortizacao'].fillna(0)

    anbima_fluxo_debenture.to_excel(xlsx_path_anbima)

    #Ajusta as datas para ficar na ordem crescente
    anbima_fluxo_debenture = anbima_fluxo_debenture.sort(['codigo_isin','id_papel','dt_ref'], ascending=[True,True,True])

    #Retira dt_ref muito anteriores à primeira data de pagamento de juros
    anbima_fluxo_debenture['dif'] = anbima_fluxo_debenture['dt_ref'] - anbima_fluxo_debenture['data_primeiro_pagamento_juros']
    anbima_fluxo_debenture['dif'] = anbima_fluxo_debenture['dif']/np.timedelta64(1, 'D')
    anbima_fluxo_debenture['dif'] = anbima_fluxo_debenture['dif'].astype(int)

    anbima_fluxo_debenture = anbima_fluxo_debenture[(anbima_fluxo_debenture.dif>=0)]
    del anbima_fluxo_debenture['dif']


    ##Retira dt_ref de amortizacao muito anteriores à primeira data de pagamento de amortizacao
    #'NESTA RODADA, NENHUM PAGAMENTO DE AMT ESTÁ ANTES DA DATA DO PRIMEIRO PAGAMENTO'''

    #Recalcula os percentuais de amortização para somar 100%
    aux = anbima_fluxo_debenture[['id_papel', 'amt_tipo']].copy()
    aux = aux.groupby(['id_papel']).sum().reset_index()
    aux = aux.rename(columns={'amt_tipo': 'amt_parc'})
    anbima_fluxo_debenture = anbima_fluxo_debenture.merge(aux, on=['id_papel'], how='left')

    anbima_fluxo_debenture['perc_amortizacao'] = np.where(((anbima_fluxo_debenture['amt_tipo']==1)&(anbima_fluxo_debenture['amt_parc']!=0)),100/anbima_fluxo_debenture['amt_parc'],0)
    anbima_fluxo_debenture['perc_amortizacao'] = np.where(((anbima_fluxo_debenture['amt_tipo']==1)&(anbima_fluxo_debenture['amt_parc']==0)),100,anbima_fluxo_debenture['perc_amortizacao'])
    anbima_fluxo_debenture['perc_amortizacao'] = np.where(anbima_fluxo_debenture['amt_tipo']==0,0,anbima_fluxo_debenture['perc_amortizacao'])

    #Unifica linhas de juros e amortização pagando na mesma data
    aux = anbima_fluxo_debenture[['id_papel','codigo_isin','dt_ref','perc_amortizacao','amt_tipo']][anbima_fluxo_debenture.juros_tipo=='accrue'].copy()
    aux = aux.rename(columns={'perc_amortizacao':'perc_amortizacao1','amt_tipo':'amt_tipo1'})

    anbima_fluxo_debenture = anbima_fluxo_debenture.merge(aux,on=['id_papel','codigo_isin','dt_ref'],how='left')
    anbima_fluxo_debenture['perc_amortizacao'] = np.where(anbima_fluxo_debenture['perc_amortizacao1'].notnull(),anbima_fluxo_debenture['perc_amortizacao1'],anbima_fluxo_debenture['perc_amortizacao'])
    anbima_fluxo_debenture['amt_tipo'] = np.where(anbima_fluxo_debenture['amt_tipo1'].notnull(),anbima_fluxo_debenture['amt_tipo1'],anbima_fluxo_debenture['amt_tipo'])

    anbima_fluxo_debenture = anbima_fluxo_debenture.drop_duplicates(subset=['id_papel','codigo_isin','dt_ref','amt_tipo'])

    #Salva o arquivo de controle
    anbima_fluxo_debenture.to_excel(writer,'fluxo')
    writer.save()
    logger.info("Arquivos salvos com sucesso")

    # Agregação ao fluxo manual
    fluxo_manual = pd.read_excel(depend_path_carat, header=0)

    id_papel = aux_final[['isin','id_papel']].copy()
    id_papel = id_papel.rename(columns={'isin':'codigo_isin'})

    fluxo_manual = fluxo_manual.merge(id_papel,on=['codigo_isin'])

    fluxo_manual = fluxo_manual.sort(['id_papel','dt_ref'],ascending=[True,True])

    fluxo_manual['data_bd'] = horario_bd

    anbima_fluxo_debenture = anbima_fluxo_debenture.append(fluxo_manual)

    # Finalização
    aux_dtoperacao = aux_final[['id_papel','dtoperacao']]
    anbima_fluxo_debenture = anbima_fluxo_debenture.merge(aux_dtoperacao,on=['id_papel'],how='left')

    #Deleta colunas não necessárias para o mtm
    del anbima_fluxo_debenture['perc_amortizacao1']
    del anbima_fluxo_debenture['amt_tipo']
    del anbima_fluxo_debenture['amt_tipo1']
    del anbima_fluxo_debenture['amt_parc']
    del anbima_fluxo_debenture['flag_tipo']

    #Renomeação temporária do id da anbima
    anbima_fluxo_debenture = anbima_fluxo_debenture.rename(columns={'id_andima_debentures':'id_anbima_debentures'})

    #Reseta os indexadores
    anbima_fluxo_debenture = anbima_fluxo_debenture.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

    #Ajusta as datas para ficar na ordem crescente
    anbima_fluxo_debenture = anbima_fluxo_debenture.sort(['codigo_isin','id_papel','dt_ref'], ascending=[True,True,True])

    logger.info("Salvando base de dados - fluxo_titprivado")
    pd.io.sql.to_sql(anbima_fluxo_debenture, name='fluxo_titprivado', con=connection, if_exists='append', flavor='mysql', index=False)
    connection.close()
