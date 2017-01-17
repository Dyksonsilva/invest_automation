# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 13:22:22 2016

@author: William.Loureiro
"""
import pandas as pd
import xmltodict
import glob
import pymysql as db
import numpy as np
import urllib.request
import urllib
import bizdays
import datetime 
from selenium import webdriver 
import selenium
import os
import zipfile, time
import io
import html5lib
import warnings    
from xml.dom import minidom
import math
import sys
from pandas.tseries.offsets import *
from findt import FinDt
from bs4 import BeautifulSoup

warnings.simplefilter(action = "ignore")

#senha=""
#usuario=""
#senhabd="projetoinvbd"

exec(open("diretorios.py").read())

connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

hoje = datetime.datetime.now()

dia=dtbase[6:8]#str(hoje.day)
mes=dtbase[4:6]#str(hoje.month)
ano=dtbase[0:4]#str(hoje.year)

if len(mes)==1:
    mes="0"+mes

if len(dia)==1:
    dia="0"+dia
    
#dia='21'
#mes='10'


def robo_bmf_cotacoes(ano, mes, dia):

    # Acesso ao arquivo zip
    import requests, zipfile, io
    
    z = zipfile.ZipFile("C:/Users/"+usuario+"/Downloads/COTAHIST_D"+dia+mes+ano+".zip","r")
    z.extractall(path="C:/Users/"+usuario+"/Downloads/")
    
    #
    # Fechamento diário
    #
    
    tamanhoCampos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]
    arquivo_bovespa="C:/Users/"+usuario+"/Downloads/COTAHIST_D"+dia+mes+ano+".TXT"
    dadosAcoes=pd.read_fwf(arquivo_bovespa, widths=tamanhoCampos, header=0)
    
    #Padronizar nomes das colunas
    
    dadosAcoes.columns = [
    "tipo_registro",
    "data_pregao",
    "cod_bdi",
    "cod_negociacao",
    "tipo_mercado",
    "noma_empresa",
    "especificacao_papel",
    "prazo_dias_merc_termo",
    "moeda_referencia",
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
    "numero_negocios",
    "quantidade_papeis_negociados",
    "volume_total_negociado",
    "preco_exercicio",
    "ìndicador_correcao_precos",
    "data_vencimento" ,
    "fator_cotacao",
    "preco_exercicio_pontos",
    "codigo_isin",
    "num_distribuicao_papel"]
    
    # Eliminar a última linha ()
    linha=len(dadosAcoes["data_pregao"])
    dadosAcoes=dadosAcoes.drop(linha-1)
    
    # Colocar valores com virgula (divir por 100)
    
    listaVirgula=[
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
    "volume_total_negociado",
    "preco_exercicio",
    "preco_exercicio_pontos"
    ]
    
    for coluna in listaVirgula:
        dadosAcoes[coluna]=[i/100. for i in dadosAcoes[coluna]]
    print(dadosAcoes)
    # Warnings tamanho
    import warnings
    warnings.simplefilter(action = "ignore", category = RuntimeWarning)
    
    horario_bd = datetime.datetime.now()
    horario_bd = horario_bd.strftime("%Y-%m-%d %H:%M:%S")

    dadosAcoes['data_bd'] = horario_bd

    # Salvar informacoes no banco de dados
    import pymysql as db
    connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')
    
    pd.io.sql.to_sql(dadosAcoes, name='bovespa_cotahist', con=connection,if_exists="append", flavor='mysql', index=0, chunksize=5000)

robo_bmf_cotacoes(ano, mes, dia)