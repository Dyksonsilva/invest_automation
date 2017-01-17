# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 17:52:32 2016

@author: Cora.Santos
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Jul  3 21:30:07 2016

@author: Cora.Santos
"""

from os import listdir
from pandas import ExcelWriter
import pandas as pd
import numpy as np
import datetime, time
import pymysql as db

senhabd = 'projetoinvbd'

#root = 'C:/Users/William.Loureiro/Documents/Projeto_Fundos/Exemplo Rotina/codigo_fonte_final'

exec(open("diretorios.py").read())
end = root + '/codigo_fonte_final/'
base_dir = root +'/codigo_fonte_final/download de dados/bacen_'+dtbase+'/'
lista = listdir(base_dir)

connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

output = pd.DataFrame(columns=['data_referencia','valor','codigo','nome','frequencia','data_bd'])

for i in lista:
    aux = pd.DataFrame(columns=['data_referencia','valor','codigo','nome','frequencia','data_bd'])
    tabela = pd.read_csv(base_dir+i, skiprows=0, sep =";",header=0, encoding ="iso-8859-1")
  
    #Tira a última linha que é coisa do bacen
    tabela = tabela[0:len(tabela)-1]
    
    #Pega o número da série pelo header das colunas
    codigo = tabela.columns.values
    codigo = codigo[1]
    codigo = codigo.split('-')
    codigo = codigo[0].replace(' ','')

    #Puxa a informação da bacen_series da bd
    x = 'select * from projeto_inv.bacen_series where codigo = ' + codigo
    bacen_series = pd.read_sql(x,con=connection)    
    nome = bacen_series['nome'].iloc[0]
    frequencia = bacen_series['frequencia'].iloc[0]

    tabela.columns =['dt_ref','taxa']

    #Data de referencia
    tabela['dt_ref'] = tabela['dt_ref'].str.split('/')
    tabela['tamanho'] = tabela['dt_ref'].str.len()
    if tabela['tamanho'].iloc[0]==2:
        tabela['dt_ref'] = tabela['dt_ref'].str[1] + tabela['dt_ref'].str[0] + '01'
    else:
        tabela['dt_ref'] = tabela['dt_ref'].str[2] + tabela['dt_ref'].str[1] + tabela['dt_ref'].str[0]
    
    aux['data_referencia'] = tabela['dt_ref']
    aux['valor'] = tabela['taxa']
    aux['codigo'] = codigo
    aux['nome'] = nome
    aux['frequencia'] = frequencia
    
    output = output.append(aux)

    
#Finalização    
output['data_bd'] = datetime.datetime.today()
output['codigo'] = output['codigo'].astype(int)
output['data_referencia'] = pd.to_datetime(output['data_referencia']).dt.date
output['valor'] = output['valor'].str.replace(',','.')
output['valor'] = output['valor'].astype(float)
output['data_bd'] = datetime.datetime.today()

output['nome'] = np.where(output['codigo']==7811,'TR',output['nome'])

pd.io.sql.to_sql(output, name='bacen_series', con=connection,if_exists="append", flavor='mysql', index=False)

#Preenchimento do bc_series_hist

output = output[output.codigo.isin([7811,189,256,433,4389])].copy()

bc_series_hist = pd.DataFrame(columns=['valor','indice','ano','mes','dia','dt_ref'])

bc_series_hist['valor'] = output['valor']
bc_series_hist['indice'] = output['codigo'].astype(str)
bc_series_hist['dt_ref'] = output['data_referencia']


bc_series_hist['indice'] = bc_series_hist['indice'].str.replace('7811','TR')
bc_series_hist['indice'] = bc_series_hist['indice'].str.replace('189','IGP')
bc_series_hist['indice'] = bc_series_hist['indice'].str.replace('256','TJLP')
bc_series_hist['indice'] = bc_series_hist['indice'].str.replace('433','IPCA')
bc_series_hist['indice'] = bc_series_hist['indice'].str.replace('4389','DI1')

bc_series_hist['dt_ref1'] = bc_series_hist['dt_ref'].astype(str)
bc_series_hist['dt_ref1'] = bc_series_hist['dt_ref1'].str.split('-')
bc_series_hist['ano'] = bc_series_hist['dt_ref1'].str[0]
bc_series_hist['mes'] = bc_series_hist['dt_ref1'].str[1]
bc_series_hist['dia'] = bc_series_hist['dt_ref1'].str[2]

del bc_series_hist['dt_ref1']

#Salvar no MySQL
pd.io.sql.to_sql(bc_series_hist, name='bc_series_hist', con=connection, if_exists='append', flavor='mysql', index=0)