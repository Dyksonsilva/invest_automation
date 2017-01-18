# -*- coding: utf-8 -*-
"""
Created on Thu May 19 16:49:06 2016

@author: Cora.Santos
"""

import datetime
import pandas as pd
import numpy as np
import urllib
import urllib.request
import bizdays
import urllib
import pymysql as db
from findt import FinDt

###############################################################################
#----Declaração de constantes
###############################################################################

#usuario = ""
#senha = ""
#senhabd="projetoinvbd"
exec(open("C:/Users/gustavo.vrech@sa/Documents/Projetos/Investimentos - HDI/Códigos Execução ordenada/diretorios.py").read())
#Feriados nacionais
#var_path= "C:\\Users\\Cora.Santos\\Desktop\\HDI-Investimentos\\Base de dados\\feriados_nacionais.csv"
#Diretório onde salva os xlsx
#save_path = 'C:/Users/'+usuario+'/Desktop/HDI-Investimentos/Base de Dados'
#Diretório onde ficam os CSVs que enviei
#csv_path = 'C:/Users/'+usuario+'/Desktop/HDI-Investimentos/Base de Dados'
#Conexão com Banco de Dados
connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

###############################################################################
#----Criar proxy para acesso via rede interna
###############################################################################
#
#proxy_acesso = "http://"+usuario+":"+senha+"@brweb.sa.ey.net:8080"
#proxies = {'http': proxy_acesso}
#
#proxy = urllib.request.ProxyHandler(proxies)
#opener = urllib.request.build_opener(proxy)
#urllib.request.install_opener(opener)

###############################################################################
#----Criar o date range de busca
###############################################################################

#Inicio da série histórica
dias = 2
meses = 1
anos = 0

fim = datetime.date(datetime.date.today().year,datetime.date.today().month,datetime.date.today().day-1)
inicio = datetime.date(datetime.date.today().year-anos,datetime.date.today().month-meses,datetime.date.today().day-dias)

per = FinDt.DatasFinanceiras(inicio, fim, path_arquivo=var_path)

du = pd.DataFrame(columns=['dt_ref'])
du['dt_ref'] = per.dias(3)
du['dt_ref'] = du['dt_ref'].astype(str)
du['dia'] = du['dt_ref'].str[8:10]
du['mes'] = du['dt_ref'].str[5:7]
du['ano'] = du['dt_ref'].str[0:4]

###############################################################################
#----Busca histórica
###############################################################################

dadosDebentures = pd.DataFrame()

for i in range(0,len(du)):
    dia = du['dia'].iloc[i]
    mes = du['mes'].iloc[i]
    ano = du['ano'].iloc[i] 
    pagina_debentures_anbima="http://www.anbima.com.br/merc_sec_debentures/arqs/db"+ano[2:]+mes+dia+".txt"
    try:
        paginaDebentures = urllib.request.urlopen(pagina_debentures_anbima)
        dadosDebentures_temp = pd.read_table(paginaDebentures,sep='@', header=1, encoding="iso-8859-1")
        # Colocar data de referência
        dadosDebentures_temp["data_referencia"]=ano+'-'+mes+'-'+dia
        dadosDebentures = dadosDebentures.append(dadosDebentures_temp)
        print(dia,mes,ano)
    except urllib.error.HTTPError as e: print('No data: '+dia+'-'+mes+'-'+ano);ResponseData = ''
    except urllib.error.URLError as e: print('Could not wait: '+dia+'-'+mes+'-'+ano);ResponseData = ''

###############################################################################
#----Manipulação da base
###############################################################################

horario_bd=datetime.datetime.now()
dadosDebentures["data_bd"]=horario_bd

print(dadosDebentures.columns)

## Padronizar nomes das colunas   
dadosDebentures.columns=[
"codigo",
"nome",
"repac_vencimento",
"indice_correcao",
"taxa_compra",
"taxa_venda",
"taxa_indicativa",
"desvio_padrao",
"intervalo_indicativo_minimo",
"intervalo_indicativo_maximo",
"pu",
"perc_pu_par",
"perc_reunie",
"duration",
"referencia_ntnb",
"data_referencia",
"data_bd"]


## Trocar virgula por ponto e "--" por None
dadosDebentures=dadosDebentures.replace({np.nan: None}, regex=True)
dadosDebentures=dadosDebentures.replace({',': '.'}, regex=True)
dadosDebentures=dadosDebentures.replace({'--': None}, regex=True)
dadosDebentures=dadosDebentures.replace({'N/D': None}, regex=True)

# Converter formato data
dadosDebentures["repac_vencimento"]=pd.to_datetime(dadosDebentures["repac_vencimento"], format="%d/%m/%Y")
dadosDebentures["referencia_ntnb"]=pd.to_datetime(dadosDebentures["referencia_ntnb"], format="%d/%m/%Y")
dadosDebentures_temp["data_referencia"] = pd.to_datetime(dadosDebentures_temp["data_referencia"], format="%Y-%m-%d")
       
dadosDebentures = dadosDebentures.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')       
       
# Salvar na base de dados
connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

pd.io.sql.to_sql(dadosDebentures, name='anbima_debentures', con=connection,if_exists="append", flavor='mysql', index=0)