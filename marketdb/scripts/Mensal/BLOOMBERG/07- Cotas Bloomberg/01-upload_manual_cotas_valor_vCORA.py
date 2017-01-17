# -*- coding: utf-8 -*-
"""
Created on Sat Jul  2 13:31:15 2016

@author: Felipe.Tumenas
"""

import os
import pandas as pd
import pymysql as db
import datetime
import numpy as np
from findt import FinDt

exec(open("diretorios.py").read())

usuario = "william.loureiro"
senha = "Testedoti4"

var_path= root+"/codigo_fonte_final/feriados_nacionais.csv"

## Conexão com o BD
senhabd = "projetoinvbd"
connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

###############################################################################
#Criação série diária
###############################################################################

#Seleciona o última dia do mês vigente
mesfim=datetime.date.today().month
fim=datetime.date(datetime.date.today().year,mesfim,1)-pd.DateOffset(months=0, days=1)

dt_ref = pd.date_range (start='01/01/1996', end=fim, freq='D').date
ano = pd.date_range (start='01/01/1996', end=fim, freq='D').year
mes = pd.date_range (start='01/01/1996', end=fim, freq='D').month
dias = pd.date_range (start='01/01/1996', end=fim, freq='D').day
serie_dias = pd.DataFrame(columns=['dt_ref', 'ano', 'mes','dia'])
serie_dias['dt_ref']=dt_ref
serie_dias['ano']=ano
serie_dias['mes']=mes
serie_dias['dia']=dias

#identificar se é dia útil

dt_max=max(serie_dias['dt_ref'])
dt_min=min(serie_dias['dt_ref'])
per = FinDt.DatasFinanceiras(dt_min, dt_max, path_arquivo=var_path)

du=pd.DataFrame(columns=['dt_ref'])
du['dt_ref']=per.dias(3)
du['du_1']=1

serie_dias=serie_dias.merge(du, on=['dt_ref'], how='left')
serie_dias['du_1']=serie_dias['du_1'].fillna(0)
serie_dias['dc_1']=1

#calculo de dias corridos por mes
x = serie_dias[['dt_ref','ano','mes']].groupby(['ano','mes']).agg(['count'])
x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
x1 = pd.DataFrame(columns=['ano', 'mes', 'dc'])

x1['ano'] = x['ano']
x1['mes'] = x['mes']
x1['dc'] = x['dt_ref']

serie_dias=serie_dias.merge(x1, on = ['ano', 'mes'], how='left')

#calculo de dias uteis por mes
y = serie_dias[['du_1','ano','mes']].groupby(['ano','mes']).agg(['sum'])
y = y.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
y1 = pd.DataFrame(columns=['ano', 'mes', 'du'])

y1['ano'] = y['ano']
y1['mes'] = y['mes']
y1['du'] = y['du_1']

serie_dias=serie_dias.merge(y1, on = ['ano', 'mes'], how='left')

###############################################################################
#Criação da série de cotas
###############################################################################

path = root+'/codigo_fonte_final/download de dados/Cotas_'+dtbase
lista = os.listdir(path)

lista = [ x for x in lista if "~" not in x ]

cotas = pd.DataFrame()

for i in lista:
    leitura = pd.read_excel(path+'/'+i)
    leitura = leitura.rename(columns={'Date':'dt_ref','FUND_NET_ASSET_VAL':'cota','FUND_TOTAL_ASSETS':'pl'})
    leitura['isin_fundo'] = i
#    del leitura['Cap. Líquida']
#    del leitura['Num. Cotistas']
#    del leitura['Var% no dia']
#    del leitura['Var% no mês']
#    del leitura['Var% no ano']
#    del leitura['Var% em 12 meses']    
    cotas = cotas.append(leitura)    

cotas['isin_fundo'] = cotas['isin_fundo'].str.split('.')
cotas['isin_fundo'] = cotas['isin_fundo'].str[0]

#cotas_aux = cotas.copy()
#cotas = cotas_aux.copy()

cotas['dt_ref'] = cotas['dt_ref'].astype(str)
cotas['dt_ref'] = cotas['dt_ref'].str.split('-')
cotas['ano'] = cotas['dt_ref'].str[0]
cotas['mes'] = cotas['dt_ref'].str[1]
cotas['dia'] = cotas['dt_ref'].str[2]
cotas['dt_ref'] = pd.to_datetime(cotas['ano']+cotas['mes']+cotas['dia']).dt.date
del cotas['ano']
del cotas['mes']
del cotas['dia']

cnpj = pd.read_sql_query('SELECT DISTINCT isin, cnpjfundo_outros, cnpjfundo_1nivel from projeto_inv.xml_quadro_operacoes where produto = "fundo"', connection)

cnpj['cnpjfundo_outros'] = np.where(cnpj['cnpjfundo_outros'].isnull(),cnpj['cnpjfundo_1nivel'],cnpj['cnpjfundo_outros'])
cnpj['cnpj'] = cnpj['cnpjfundo_outros']
del cnpj['cnpjfundo_1nivel']
del cnpj['cnpjfundo_outros']

cnpj = cnpj.drop_duplicates()

cotas = pd.merge(cotas,cnpj,left_on='isin_fundo',right_on='isin',how='left')
del cotas['isin']

cotas = cotas.rename(columns={'cnpj':'cnpj_fundo'})
cotas['data_bd'] = datetime.datetime.today()

pd.io.sql.to_sql(cotas, name='valoreconomico_cotas', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)    


################################################################################
##Verificação de qualidade da série de cotas
################################################################################
#
#mes_hoje = datetime.date(2016,7,29)
#hoje = mes_hoje
#mes_hoje = str(mes_hoje)
#ano_hoje = int(mes_hoje[0:4])
#mes_hoje = int(mes_hoje[5:7])
#
#serie_dias = serie_dias[['dt_ref','du_1','du','ano','mes']][(serie_dias['ano']<ano_hoje)|((serie_dias['ano']==ano_hoje)&(serie_dias['mes']<=mes_hoje))].copy()
#serie_dias = serie_dias[serie_dias['dt_ref']!=hoje]
#
#cotas = pd.merge(cotas,serie_dias,left_on='dt_ref',right_on='dt_ref',how='left')
#
#cotas=cotas.rename(columns={'cnpj_fundo':'cnpj'})
#
#verificacao = cotas[['isin_fundo','dt_ref','cnpj','du_1','du','ano','mes']].copy()
#verificacao = verificacao.sort(['isin_fundo','ano','mes'],ascending=[True,False,False])
#verificacao = verificacao[verificacao['du_1'].notnull()].copy()
#
#x = verificacao[['isin_fundo','du_1','ano','mes']].groupby(['isin_fundo','ano','mes']).agg(['sum'])
#x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
#x1 = pd.DataFrame(columns=['isin_fundo', 'ano', 'mes','du_obs'])
#x1['isin_fundo'] = x['isin_fundo']
#x1['ano'] = x['ano']
#x1['mes'] = x['mes']
#x1['du_obs'] = x['du_1']
#
#verificacao = pd.merge(verificacao,x1,right_on=['isin_fundo','ano','mes'],left_on=['isin_fundo','ano','mes'],how='left')
#verificacao['count'] = verificacao[['isin_fundo','dt_ref']].groupby(['isin_fundo']).agg(['cumcount'])
#
#
#
#verificacao = verificacao.sort(['isin_fundo','dt_ref'],ascending=[True,True])





