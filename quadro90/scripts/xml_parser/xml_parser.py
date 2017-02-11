def xml_parser():

    import pandas as pd
    import xmltodict
    import glob
    import pymysql as db
    import numpy as np
    import datetime#,time
    import logging

    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database
    from quadro90.scripts.xml_parser.dic_tabela import dic_tabela

    logger = logging.getLogger(__name__)

    # Retorna um array (ano, mes e dia) referente ao útimo dia útil do mês anterior configurado no banco de dados
    array_data = get_data_ultimo_dia_util_mes_anterior()

    #Diretório com os arquivos do cliente:
    full_path = full_path_from_database("xml_dir")
    xml_dir = full_path+'ano_'+array_data[0]+'/'+'XML_'+str(array_data[0])+str(array_data[1])+str(array_data[2])
    #xml_dir = full_path_from_database("xml_dir")

    # Diretório de saída dos arquivos:
    output_path = full_path_from_database("get_output_quadro90")

    logger.info("Conectando no Banco de dados")

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Tratando dados")

    # Função para transformar os dados para forma tabular
    query= 'select max(header_id) as header_id from projeto_inv.xml_header_org';

    logger.info("Lendo DB")

    qtde=pd.read_sql(query, con=connection)
    if qtde['header_id'][0]==None:
        n_header_id=0
    else:
        n_header_id=qtde.get_value(0,'header_id')+1

    lista_tabelas=[
    'header',
    'partplanprev',
    'titpublico',
    'titprivado',
    'debenture',
    'acoes',
    'opcoesacoes',
    'opcoesderiv',
    'opcoesflx',
    'termorv',
    'termorf',
    'futuros',
    'swap',
    'caixa',
    'cotas',
    'despesas',
    'outrasdespesas',
    'provisao',
    'corretagem']

    ## Criação das tabelas
    for x in lista_tabelas:
        globals()['tabela_%s' % x] = pd.DataFrame() #cria tabelas usando como nome tabela_+"string de lista_tabelas"

    ## Leitura XML
    horario_bd = datetime.datetime.now()
    horario_bd = horario_bd.strftime("%Y-%m-%d %H:%M:%S")

    lista_arquivos= glob.glob(xml_dir + '/*.*')

    for arquivo in lista_arquivos:
        xml=open(arquivo, "r")
        original_doc= xml.read()
        xmlFundo=xmltodict.parse(original_doc)

        #Encontra índice para separar o nome do arquivo
        start_string=arquivo.index('XML_'+str(array_data[0])+str(array_data[1])+str(array_data[2]))

        logger.info("Lendo XML "+arquivo[(start_string+13):])

        header_id=len(tabela_header)
        try:
            endereco=xmlFundo['arquivoposicao_4_01']['fundo']
        except:
            endereco=xmlFundo['arquivoposicao_4_01']['carteira']

        for item in endereco:
            for x in range(len(lista_tabelas)):#utiliza range para x ser indice da lista, tornando o uso do if possivel
                if item == lista_tabelas[x]:
                    y=x
                    x=lista_tabelas[x]#x recebe a string em lista tabelas, para acessar os dataframes com os nomes contidos em lista tabelas
                    globals()['tabela_%s' % x]=dic_tabela(endereco, lista_tabelas[y], globals()['tabela_%s' % x], header_id)

    #Trara os NaN
    for y in range(len(lista_tabelas)):
        x=lista_tabelas[y]
        globals()['tabela_%s' % x] = globals()['tabela_%s' % x].replace('', np.nan)
        globals()['tabela_%s' % x] = globals()['tabela_%s' % x].where((pd.notnull(globals()['tabela_%s' % x])), None)

    #Data do relatório
    dt_base = globals()['tabela_%s' % 'header']['dtposicao']
    dt_base = dt_base.drop_duplicates()
    dt_base = dt_base.iloc[0]
    dt_base_str = str(dt_base).replace('-','')

    for y in range(len(lista_tabelas)):
        x=lista_tabelas[y]
        if len(globals()['tabela_%s' % x])!=0:
            #print(x,'###',globals()['tabela_%s' % x].columns)
            globals()['tabela_%s' % x].loc[len(x),'data_bd'] = None
            globals()['tabela_%s' % x]['data_bd'] = globals()['tabela_%s' % x]['data_bd'].fillna(horario_bd)

            try:
                globals()['tabela_%s' % x]['header_id'] = globals()['tabela_%s' % x]['header_id']+n_header_id
                globals()['tabela_%s' % x]['header_id'] = globals()['tabela_%s' % x]['header_id'].astype(int)
            except:
                pass
                #print(x)

        if ((x=='titpublico')|(x=='titprivado')|(x=='debenture')):
            try:
                globals()['tabela_%s' % x]['header_id_aux'] = globals()['tabela_%s' % x]['header_id'].astype(str)
                globals()['tabela_%s' % x]['index_col'] = globals()['tabela_%s' % x].index.values
                globals()['tabela_%s' % x]['index_col'] = globals()['tabela_%s' % x]['index_col'].astype(str)
                globals()['tabela_%s' % x]['id_papel'] = dt_base_str +'_'+globals()['tabela_%s' % x]['index_col']+'_'+globals()['tabela_%s' % x]['header_id_aux']
                del globals()['tabela_%s' % x]['index_col']
                del globals()['tabela_%s' % x]['header_id_aux']
            except:
                pass
                #print(x)

    logger.info("Corrigindo tabela Título Privado")
    col='titprivado'

    if len(globals()['tabela_%s' % col]) != 0:
        aux = globals()['tabela_%s' % col][['id_papel', 'isin']][
            globals()['tabela_%s' % col]['isin'].isin(['************', 'BR**********'])].copy()
        aux['idx'] = np.linspace(0, len(aux) - 1, len(aux))
        aux['idx'] = aux['idx'].astype(int)
        aux['idx'] = aux['idx'].astype(str)
        aux['isin_novo'] = 'TIT000_BR' + aux['idx']
        aux['isin_novo'] = aux['isin_novo'].str.zfill(13)
        aux['isin_novo'] = aux['isin_novo'].str.split('_')
        aux['isin_novo'] = aux['isin_novo'].str[1] + aux['isin_novo'].str[0]

        del aux['isin']
        del aux['idx']

        globals()['tabela_%s' % col] = globals()['tabela_%s' % col].merge(aux, on=['id_papel'], how='left')
        globals()['tabela_%s' % col]['isin'] = np.where(globals()['tabela_%s' % col]['isin_novo'].notnull(),
                                                        globals()['tabela_%s' % col]['isin_novo'],
                                                        globals()['tabela_%s' % col]['isin'])

        del globals()['tabela_%s' % col]['isin_novo']

    logger.info("Corrigindo tabela debenture")
    col = 'debenture'

    if len(globals()['tabela_%s' % col]) != 0:
        aux = globals()['tabela_%s' % col][['id_papel', 'isin']][globals()['tabela_%s' % col]['isin'].isin(['************', 'BR**********'])].copy()
        aux['idx'] = np.linspace(0, len(aux) - 1, len(aux))
        aux['idx'] = aux['idx'].astype(int)
        aux['idx'] = aux['idx'].astype(str)
        aux['isin_novo'] = 'TIT000_BR' + aux['idx']
        aux['isin_novo'] = aux['isin_novo'].str.zfill(13)
        aux['isin_novo'] = aux['isin_novo'].str.split('_')
        aux['isin_novo'] = aux['isin_novo'].str[1] + aux['isin_novo'].str[0]

        del aux['isin']
        del aux['idx']

        globals()['tabela_%s' % col] = globals()['tabela_%s' % col].merge(aux, on=['id_papel'], how='left')
        globals()['tabela_%s' % col]['isin'] = np.where(globals()['tabela_%s' % col]['isin_novo'].notnull(),globals()['tabela_%s' % col]['isin_novo'],globals()['tabela_%s' % col]['isin'])

        del globals()['tabela_%s' % col]['isin_novo']

    x = 'header'
    globals()['tabela_%s' % x]['cnpj'] = globals()['tabela_%s' % x]['cnpj'].fillna(0)
    globals()['tabela_%s' % x]['cnpj'] = globals()['tabela_%s' % x]['cnpj'].astype(str)
    globals()['tabela_%s' % x]['cnpj'] = globals()['tabela_%s' % x]['cnpj'].str.zfill(14)

    logger.info("Salvando fundos")
    globals()['tabela_%s' % x][['cnpj','nome']].to_excel(output_path+'fundos.xlsx')

    for y in range(len(lista_tabelas)):
        x = lista_tabelas[y]
        globals()['tabela_%s' % x].to_excel(output_path+'tabela_'+lista_tabelas[y]+'_'+dt_base_str+'.xlsx', encoding='utf-8', index=True, index_label="id")

        logger.info("Salvando tabela " + x + " no banco de dados")
        pd.io.sql.to_sql(globals()['tabela_%s' % x], name='xml_'+lista_tabelas[y]+'_org', con=connection,if_exists="append", flavor='mysql', index=0)

    logger.info("Base de dados salva com sucesso")
    connection.close()