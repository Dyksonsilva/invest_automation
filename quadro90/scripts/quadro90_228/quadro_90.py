def quadro90(data_relat, cnpj):

    import logging
    import pandas as pd
    import pymysql as db
    import numpy

    from quadro90.scripts.quadro90_228.quadro_oper import quadro_oper
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from dependencias.Metodos.funcoes_auxiliares import get_global_var
    from datetime import date

    #Define variaveis
    logger = logging.getLogger(__name__)
    limite_fgc = int(get_global_var("limite_fgc"))

    #Chama quadro de operações
    qo = quadro_oper(data_relat, cnpj)

    ano = data_relat[0:4]
    mes = data_relat[5:7]
    dia = data_relat[8:10]

    dt_relat = date(int(ano), int(mes), int(dia))

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    #Load Header ID
    curr_querry='select distinct a.cnpj, a.header_id, a.dtposicao, a.data_bd from projeto_inv.xml_header_org a right join (select cnpj, dtposicao,max(data_bd) as data_bd from projeto_inv.xml_header_org where dtposicao="'+ data_relat + '" group by 1,2) b on a.cnpj=b.cnpj and  a.data_bd=b.data_bd;'
    df = pd.read_sql(curr_querry, con=connection)
    logger.info("Leitura do banco de dados executada com sucesso")

    if len(df) == 0:
        curr_querry='select distinct a.cnpjcpf, a.header_id, a.dtposicao, a.data_bd from projeto_inv.xml_header_org a right join (select cnpjcpf, dtposicao,max(data_bd) as data_bd from projeto_inv.xml_header_org where dtposicao="'+data_relat+'" group by 1,2) b on a.cnpjcpf=b.cnpjcpf and a.data_bd=b.data_bd;'
        df = pd.read_sql(curr_querry, con=connection)
        logger.info("Leitura do banco de dados executada com sucesso")

    #Fecha conexão
    connection.close()

    #Load Numeraca
    numeraca_path = full_path_from_database('excels')
    numeraca_path = numeraca_path+'numeraca.xlsx'
    numeraca = pd.read_excel(numeraca_path, sheetname=0)
    logger.info("Leitura do arquivo executada com sucesso")

    logger.info("Tratando dados")

    #Define Instituição Financeira
    numeraca['IF'] = 0
    siglas_IF = numeraca[numeraca['Sigla'].str.contains(r'DPGE\s|CDB\s|CCB\s|LH\s|LCI\s|LCA\s')]
    descr_IF = numeraca[numeraca['Descrição'].str.contains(r'LETRA FINANCEIRA\s|LETRA DE CAMBIO\s')]
    numeraca.set_value(siglas_IF.index,'IF',1)
    numeraca.set_value(descr_IF.index,'IF',1)

    #Corrige Tipo de Ativo com 0 no final:
    numeraca['Tipo de Ativo']=numeraca['Tipo de Ativo'].str.lstrip(' ').str.rstrip(' ')
    numeraca.set_value(numeraca[numeraca['Tipo de Ativo'].str.len()==1].index,'Tipo de Ativo',numeraca[['Tipo de Ativo']].astype(str)+'00')
    numeraca.set_value(numeraca[numeraca['Tipo de Ativo'].str.len()==2].index,'Tipo de Ativo',numeraca[['Tipo de Ativo']].astype(str)+'0')

    #Corrige Categoria
    numeraca['Categoria']=numeraca['Categoria'].str.lstrip(' ').str.rstrip(' ')

    #Define tipo de ativo no quadro:
    qo['tipo_ativo'] = qo['isin'].str[6:9]

    #Define categoria no quadro:
    qo['id_q90'] = 0

    #Delta 3 meses:
    qo['dif'] = dt_relat - qo['dt_vencto']
    # linha abaixo necessária para PyCharm, pois ele não reconhece a referência timedelta64.
    # noinspection PyUnresolvedReferences
    qo['dif'] = qo['dif']/(numpy.timedelta64(1, 'D'))
    qo['dif'] = abs(qo['dif'])

    #Classifica Produtos:
    qo['cat_produto']=qo['produto']
    qo['cat_produto']=numpy.where(qo.produto.str.contains('despesa'), 'despesa', qo['cat_produto'])
    qo['cat_produto']=numpy.where(qo.produto.str.contains('Termo'), 'termo', qo['cat_produto'])
    qo['cat_produto']=numpy.where(qo.produto.str.contains('Opções'), 'opções', qo['cat_produto'])

    #Define Tesouraria
    #Fora tesouraria:
    qo1=qo[((qo['isin'].notnull())|(qo['produto']=='valores a receber'))&(qo['produto']!='ajuste de futuro')]
    #Para tesouraria:
    qo2=qo[((qo['isin'].isnull())&(qo['produto']!='valores a receber'))|(qo['produto']=='ajuste de futuro')]

    ##Cruzamento
    #03. Aplicações em mercado Aberto
    df_03=qo1[qo1['produto'].str.contains(r'compromissada - debênture|compromissada: titulo público')]
    qo.set_value(df_03.index,'id_q90',3)

    # 06. Tít priv de RF, com prazo de venc até 3 meses, emitidos por instituição financ
    left=qo1[['mtm_info','tipo_ativo','dt_vencto']][(qo1['dif']<=90) & (qo1['produto']=='título privado') & (qo1['tipo_ativo']!='DP0')]
    right=numeraca[['Tipo de Ativo','IF']][(numeraca['IF']==1)&(numeraca['Categoria']=='Renda Fixa')]
    left.reset_index(inplace=True)
    right.reset_index(inplace=True)
    df_06=pd.merge(left,right,left_on=['tipo_ativo'],right_on=['Tipo de Ativo'],how='inner')
    qo.set_value(df_06.set_index('index_x').index,'id_q90',6)

    #Adicional para outros LF e CDBs:
    df_06_ad=qo1[['mtm_info','tipo_ativo','dt_vencto']][((qo1['dif']<=90) & (qo1['produto']=='título privado')&(qo1['tipo_ativo']!='DP0'))&((qo1['ativo'].str.contains('CDB'))|(qo1['ativo'].str.contains('LF'))|(qo1['tipo_ativo'].str.contains(r'C\d\d')))]
    qo.set_value(df_06_ad.index,'id_q90',6)

    #08. Valores aplicados em DPGE garantidos pelo FGC ou c/ prazo de venc. até 3 meses (CMPID 07402):
    left=qo1[['mtm_info','tipo_ativo']][(qo1['produto']=='título privado') & ((qo1['tipo_ativo']=='DP0')|(qo1['ativo'].str.contains(r'DPGE')))]
    right=numeraca[['Tipo de Ativo']][numeraca['IF']==1]
    left.reset_index(inplace=True)
    right.reset_index(inplace=True)
    df_08=pd.merge(left,right,left_on=['tipo_ativo'],right_on=['Tipo de Ativo'],how='inner')
    qo.set_value(df_08.set_index('index_x').index,'id_q90',8)

    #vefifica limite
    if df_08['mtm_info'].sum() <= limite_fgc:
        val_a8=df_08['mtm_info'].sum()
    else:
        val_a8='ACIMA DO LIMITE'

    # 12. Tít priv de RF, com prazo de venc > 3 meses, emitidos por instituição financ
    left=qo1[['mtm_info','tipo_ativo','dt_vencto']][(qo1['dif']>90) & ((qo1['produto']=='título privado')|((qo1['ativo'].str.contains('CDB'))|(qo1['ativo'].str.contains('LF')))) & (qo1['tipo_ativo']!='DP0')]
    right=numeraca[['Tipo de Ativo']][(numeraca['IF']==1)&(numeraca['Categoria']=='Renda Fixa')]
    left.reset_index(inplace=True)
    right.reset_index(inplace=True)
    df_12=pd.merge(left,right,left_on=['tipo_ativo'],right_on=['Tipo de Ativo'],how='inner')
    qo.set_value(df_12.set_index('index_x').index,'id_q90',12)
    #Adicional para outros LF e CDBs:
    df_12_ad=qo1[['mtm_info','tipo_ativo','dt_vencto']][((qo1['dif']>90) & (qo1['produto']=='título privado')&(qo1['tipo_ativo']!='DP0'))&((qo1['ativo'].str.contains('CDB'))|(qo1['ativo'].str.contains('LF'))|(qo1['tipo_ativo'].str.contains(r'C\d\d')))]
    qo.set_value(df_12_ad.index,'id_q90',12)

    #24. Títulos privados de RF emitidos por instituição não financeira (CMPID 07418): + DEBENTURES
    left=qo1[['mtm_info','tipo_ativo']][(qo1['produto']=='título privado')|(qo1['ativo'].str.contains('CDB'))|(qo1['ativo'].str.contains('LF'))|(qo1['produto']=='debênture')]
    right=numeraca[['Tipo de Ativo','Categoria']][(numeraca['IF']==0)&(numeraca['Categoria']=='Renda Fixa')]
    left.reset_index(inplace=True)
    right.reset_index(inplace=True)
    df_24=pd.merge(left,right,left_on=['tipo_ativo'],right_on=['Tipo de Ativo'],how='inner')
    qo.set_value(df_24.set_index('index_x').index,'id_q90',24)

    #26. Títulos de RV, não classificados como ações, derivativos e ouro (CMPID 07420): + TERMO
    df_26=qo1[['mtm_info','tipo_ativo']][(qo1['produto']=='Termo_RF_IAP')|(qo1['produto']=='Termo_RV_BBSE3')|(qo1['produto']=='Termo_RV_CIEL3')|(qo1['produto']=='Termo_RV_CIEL3')|(qo1['produto']=='Termo_RV_DAGB33')]
    qo.set_value(df_26.index,'id_q90',26)

    #37. Títulos à receber
    df_37=qo1[['mtm_info','produto']][(qo1['produto']=='valores a receber')]
    qo.set_value(df_37.index,'id_q90',37)

    #44. Quotas de Fundos de Investimento
    df_44=qo1[['mtm_info','produto']][(qo1['produto']=='fundo')]
    qo.set_value(df_44.index,'id_q90',44)

    ##LINHAS ADICIONAIS - tesouraria, titulos publicos federais, acoes, opcoes, swaps, futuro
    #missing isin (exceto valores a receber), caixa, ajuste de futuro, despesas: vai para tesouraria
    #48. Tesouraria
    df_48=qo2
    qo.set_value(df_48.index,'id_q90',48)

    #49. Títulos públicos federais
    df_49=df_49=qo1[['mtm_info','produto','tipo_ativo','ativo']][(qo1['produto']=='titulo público')]
    qo.set_value(df_49.index,'id_q90',49)

    #50. Ações
    df_50=df_50=qo1[['mtm_info','produto']][(qo1['produto']=='ações')]
    qo.set_value(df_50.index,'id_q90',50)

    #51. Opção
    df_51=df_51=qo1[['mtm_info','produto']][(qo1['produto']=='opções')]
    qo.set_value(df_51.index,'id_q90',51)

    #52. Swaps
    df_52=df_52=qo1[['mtm_info','produto']][(qo1['produto']=='swap')]
    qo.set_value(df_52.index,'id_q90',52)

    #53. Futuro
    df_53=df_53=qo1[['mtm_info','produto']][(qo1['produto']=='Futuro')]
    qo.set_value(df_53.index,'id_q90',53)

    #ERROS:
    qo_err=qo[qo['id_q90']==0]
    #if qo_err['isin'].str.len() < 9:

    #Para homologação:
    qo_homol=qo[(qo['cnpjfundo_1nivel'].notnull())&(qo['fundo'].str.contains(r'^((?!fidic).)*$'))&(qo['fundo'].str.contains(r'^((?!direitos).)*$'))]

    #Salva resultados
    output_path=full_path_from_database('get_output_quadro90')
    writer = pd.ExcelWriter(output_path+'resultado_'+cnpj+"-"+data_relat+'_.xlsx', engine='xlsxwriter')
    qo.to_excel(writer, sheet_name='Resultado',index=False)
    logger.info('Arquivos salvos com sucesso')

    #'''SE O ID_Q90 == 0, preste atenção, e reclassifique manualmente'''
    desc = {'id_q90': [3,6,8,12,24,26,37,44,48,49,50,51,52,53,0],
            'descricao': ['03. Aplicações no mercado aberto',
                          '06. Tit. Priv. de RF, Instituição Financeira até 3 meses',
                          '08. DPGE',
                          '12. Tit. Priv. de RF, Instituição Financeira > 3 meses',
                          '24. Tit. Priv. de RF, Não Instituição Financeira',
                          '26. Títulos de RV. não ações (inclui Termo)',
                          '37. Títulos e créditos a receber',
                          '44. Quotas de Fundos de Investimento',
                          'Tesouraria',
                          'Títulos públicos federais',
                          'Ações',
                          'Opções',
                          'Swap',
                          'Futuro',
                          'Outros'],
            'fator_ponderacao': [0.2,
                                 0.2,
                                 0.2,
                                 0.5,
                                 1,
                                 1,
                                 1,
                                 1,
                                 0,
                                 0,
                                 0,
                                 0,
                                 0,
                                 0,
                                 0]}
    descricao=pd.DataFrame(desc)

    qo_res=pd.merge(qo, descricao, left_on='id_q90', right_on='id_q90', how='left')
    qo_res=qo_res[['id_q90', 'descricao', 'fator_ponderacao','quantidade', 'mtm_info']]
    qo_resumo=qo_res.groupby(['id_q90', 'descricao', 'fator_ponderacao'], as_index=False).sum()
    qo_resumo=qo_resumo.reindex(columns=['id_q90', 'descricao', 'mtm_info','fator_ponderacao','quantidade'])

    qo_resumo.to_excel(writer, sheet_name='Resumo', index=False, header=['Id', 'Descrição da Conta', 'Exposição', 'Fator de Ponderação', '#Ativos'])
    logger.info('Arquivos salvos com sucesso')

    #Advanced Formatting
    wb = writer.book
    ws = writer.sheets['Resumo']

    #Create Formats:
    currency = wb.add_format({'num_format': '$#,##0'})
    ws.set_column('B2:C16', 11,currency)

    percent = wb.add_format({'num_format': '0.00%'})
    ws.set_column('D2:D16', 11, percent)

    writer.save()

    # - VALIDAÇÃO

    writer = pd.ExcelWriter(output_path+"validação_q90_"+cnpj+"-"+data_relat+'_.xlsx', engine='xlsxwriter')

    qo = qo.sort(['id_q90'],ascending=True)

    lista = qo['id_q90'].unique()

    for i in lista:
        qo[qo['id_q90']==i].to_excel(writer,'id_q90_'+str(i))
        logger.info('Arquivos salvos com sucesso')

    writer.save()
