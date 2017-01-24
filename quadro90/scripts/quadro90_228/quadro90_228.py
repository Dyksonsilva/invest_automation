#Imports:
import os
import pandas as pd
import numpy as np
import pymysql as db
from datetime import *
import logging

from dependencias.Metodos.funcoes_auxiliares import *

#Define Functions
horario_inicio= datetime.datetime.now()
cnpj=full_path_from_database('cnpj_hdi')

#Define variables:
array_data = get_current_date_in_array()
ano = array_data[0]
mes = array_data[1]
dia = array_data[2]
limite_fgc = 20000000
currXLS = '\\numeraca.xlsx'

dt_relat = date(int(ano), int(mes), int(dia))

#Define directory
#root = 'C:/Users/William.Loureiro/Documents/Projeto_Fundos/Exemplo Rotina/codigo_fonte_final'
fluxo_manual_dir = root + '/codigo_fonte_final'
base_dir = root +'/codigo_fonte_final/Validacoes/'
xlsx_path = base_dir+'controle_debentures.xlsx'    
os.chdir(root+'/codigo_fonte_final/')
end_rel = root+'/codigo_fonte_final/Relatorios/'
wdPath=os.getcwd()

#Cria conexão com DB
connection=db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv', charset='utf8')

#quadro90_function

lista_cnpj=['29980158000157',
            '18096627000153']

quadro90(data_relat, full_path_from_database('cnpj_hdi'))
quadro90(data_relat, full_path_from_database('cnpj_gerling'))

horario_fim = datetime.datetime.now()
tempo=horario_fim-horario_inicio
print(tempo)