import pymysql as db
import pandas as pd
import datetime
from invest_automation.settings import BASE_DIR

def get_data_ultimo_dia_util_mes_anterior():
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    # Query que retorna valor do último dia útil do mês anterior
    dtbase = pd.read_sql_query(
        'SELECT variable_value from projeto_inv.global_variables where variable_name = "dtbase"', connection)

    dtbase = str(dtbase["variable_value"].iloc[0])

    dia = dtbase[6:8]
    mes = dtbase[4:6]
    ano = dtbase[0:4]

    connection.close()
    return ano, mes, dia

def full_path_from_database(filename):

    global BASE_DIR

    #Conexão com Banco de Dados
    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    path_isinp = pd.read_sql_query(
        'SELECT path from projeto_inv.paths where filename = ''"' + filename + '"', connection)

    BASE_DIR = BASE_DIR.replace('\\', '/')

    full_path = BASE_DIR + str(path_isinp["path"].iloc[0])

    connection.close()
    return full_path

def get_current_date_in_array():

    hoje = datetime.datetime.now()

    dia = str(hoje.day)
    mes = str(hoje.month)
    ano = str(hoje.year)

    if len(mes) == 1:
        mes = "0" + mes

    if len(dia) == 1:
        dia = "0" + dia

    return ano, mes, dia

def get_global_var(var):

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv')

    # Query que retorna variavel global
    global_var = pd.read_sql_query(
        'SELECT variable_value from projeto_inv.global_variables where variable_name = "'+var+'"', connection)

    global_var = str(global_var["variable_value"].iloc[0])

    connection.close()
    return global_var