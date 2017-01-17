
import urllib
import pandas as pd
import datetime
import pymysql as db

import pandas


ano = "2017"
mes = "01"
dia = "13"

connection = db.connect('localhost', user='root', passwd="root", db='projeto_inv')

pagina_debentures_anbima = "http://www.anbima.com.br/merc_sec_debentures/arqs/db" + ano[2:] + mes + dia + ".txt"

pagina_debentures = urllib.request.urlopen(pagina_debentures_anbima)
dados_debentures = pd.read_table(pagina_debentures, sep='@', header=1, encoding="iso-8859-1")

## Criar coluna com data_bd para a data de inserção no BD

horario_bd = datetime.datetime.now()
dados_debentures["data_bd"] = horario_bd

## Padronizar nomes das colunas
dados_debentures.columns = [
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
    "duration",
    "perc_reunie",
    "referencia_ntnb",
    "data_bd"
]

## Trocar virgula por ponto e "--" por None
dados_debentures = dados_debentures.replace({np.nan: None}, regex=True)
dados_debentures = dados_debentures.replace({',': '.'}, regex=True)
dados_debentures = dados_debentures.replace({'--': None}, regex=True)
dados_debentures = dados_debentures.replace({'N/D': None}, regex=True)

# Converter formato data
dados_debentures["repac_vencimento"] = pd.to_datetime(dados_debentures["repac_vencimento"], format="%d/%m/%Y")
dados_debentures["referencia_ntnb"] = pd.to_datetime(dados_debentures["referencia_ntnb"], format="%d/%m/%Y")

# Colocar data de referência
dados_debentures["data_referencia"] = ano + '-' + mes + '-' + dia
dados_debentures["data_referencia"] = pd.to_datetime(dados_debentures["data_referencia"], format="%Y-%m-%d")

dados_debentures = dados_debentures.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

pd.io.sql.to_sql(dados_debentures, name='anbima_debentures',
                 con=connection,
                 if_exists="append",
                 flavor='mysql',
                 index=0)

# Fecha conexão com o banco de dados
connection.close()
