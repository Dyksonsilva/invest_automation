def get_anbima_titpublico(ano, mes, dia):
    """
    Objetivo: robô de títulos públicos federais da base disponibilizada pela ANBIMA
    
    Exemplo de preenchimento: 
    
    robo_tit_publico('2016','02','23')
    """
    
    #######

    from pandas.tseries.offsets import DateOffset
    import urllib
    import pandas as pd
    import pymysql as db
    import datetime
    import logging

    logger = logging.getLogger(__name__)

    ### Conexão com a página ###
    #paginaTitulosPublicos = urllib.request.urlopen("http://www.anbima.com.br/merc_sec/arqs/ms160215.txt")
    
    paginaTitulosPublicos = urllib.request.urlopen("http://www.anbima.com.br/merc_sec/arqs/ms"+ str(ano)[2:4]+str(mes)+str(dia)+".txt")

    logger.info("Conexão com URL executado com sucesso")

    #fonte anbima='anb'
    anbima_tpf = pd.read_table(paginaTitulosPublicos, sep='@', header=1)

    logger.info("Leitura da página executada com sucesso")

    logger.info("Tratando dados")
    #renomear colunas
    old_names = ['Index', 'Titulo','Data Referencia','Codigo SELIC', 'Data Base/Emissao','Data Vencimento', 'Tx. Compra', 'Tx. Venda', 'Tx. Indicativas', 'PU'	, 'Desvio padrao', 'Interv. Ind. Inf. (D0)', 'Interv. Ind. Sup. (D0)', 'Interv. Ind. Inf. (D+1)', 'Interv. Ind. Sup. (D+1)', 'Criterio'] 
    new_names = ['Index', 'titulo', 'dt_referencia', 'cod_selic', 'dt_emissao', 'dt_vencto', 'tx_maxima', 'tx_minima', 'tx_indicativa', 'pu', 'desv_pad', 'inter_min_d0', 'inter_max_d0', 'inter_min_d1', 'inter_max_d1','criterio'] 
    anbima_tpf.rename(columns=dict(zip(old_names, new_names)), inplace=True)
    
    #alterar formato de datas
    anbima_tpf['dt_referencia']=pd.to_datetime(anbima_tpf['dt_referencia'], format='%Y%m%d')
    anbima_tpf['dt_emissao']=pd.to_datetime(anbima_tpf['dt_emissao'], format='%Y%m%d')
    anbima_tpf['dt_vencto']=pd.to_datetime(anbima_tpf['dt_vencto'], format='%Y%m%d')
    
    # DATA DE VENCIMENTO -> EXCLUSIVE
    anbima_tpf['dt_vencto2']=anbima_tpf['dt_vencto']-DateOffset(months=0, days=1)
    '''
    anbima_tpf['dt_referencia']=anbima_tpf['dt_referencia'].apply(lambda x: x.strftime('%d-%m-%Y'))
    anbima_tpf['dt_emissao']=anbima_tpf['dt_emissao'].apply(lambda x: x.strftime('%d-%m-%Y'))
    anbima_tpf['dt_vencto']=anbima_tpf['dt_vencto'].apply(lambda x: x.strftime('%d-%m-%Y'))
    anbima_tpf['dt_vencto2']=anbima_tpf['dt_vencto2'].apply(lambda x: x.strftime('%d-%m-%Y'))
    '''
    
    # CONVERSAO DE , PARA .   TRATAMENTO DE EXCECOES NUMERICAS E DE MISSING DATA
    anbima_tpf = anbima_tpf.replace({',': '.'}, regex=True)
    anbima_tpf = anbima_tpf.replace({' ': ''}, regex=True)
    anbima_tpf = anbima_tpf.replace({'--': None}, regex=True)

    #conversao para numerico
    anbima_tpf['tx_maxima'] = anbima_tpf['tx_maxima'].astype(float)
    anbima_tpf['tx_minima'] = anbima_tpf['tx_minima'].astype(float)
    anbima_tpf['tx_indicativa'] = anbima_tpf['tx_indicativa'].astype(float)
    anbima_tpf['pu'] = anbima_tpf['pu'].astype(float)
    anbima_tpf['desv_pad'] = anbima_tpf['desv_pad'].astype(float)
    anbima_tpf['inter_min_d0'] = anbima_tpf['inter_min_d0'].astype(float)
    anbima_tpf['inter_max_d0'] = anbima_tpf['inter_max_d0'].astype(float)
    anbima_tpf['inter_min_d1'] = anbima_tpf['inter_min_d1'].astype(float)
    anbima_tpf['inter_max_d1'] = anbima_tpf['inter_max_d1'].astype(float)
    
    horario_bd = datetime.datetime.now()
    anbima_tpf["dt_carga"] = horario_bd

    logger.info("Conectando no Banco de dados")

    # Conexão com Banco de Dados
    connection = db.connect('localhost', user='root', passwd="root", db='projeto_inv')

    logger.info("Conexão com DB executada com sucesso")

    logger.info("Salvando dados no DB")
    #Salvar no MySQL
    pd.io.sql.to_sql(anbima_tpf,
                     name='anbima_tpf',
                     con=connection,
                     if_exists='append',
                     flavor='mysql',
                     index=0)

    logging.info("Dados salvos no DB com sucesso")

    # Fecha conexão
    connection.close()