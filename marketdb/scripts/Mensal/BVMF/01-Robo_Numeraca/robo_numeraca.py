# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 13:47:11 2016

@author: William.Loureiro
"""
import pandas as pd
#import xmltodict
#import glob
import pymysql as db
#import numpy as np
#import urllib.request
#import urllib
#import bizdays
import datetime 
#from selenium import webdriver 
#import selenium
import os
import zipfile#, time
import io
#import html5lib
import warnings    
#from xml.dom import minidom
#import math
#import sys
#from pandas.tseries.offsets import *
#from findt import FinDt
#from bs4 import BeautifulSoup

warnings.simplefilter(action = "ignore")

#senha=""
#usuario=""
senhabd="projetoinvbd"

exec(open("diretorios.py").read())

connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

hoje = datetime.datetime.now()

dia=str(hoje.day)
mes=str(hoje.month)
ano=str(hoje.year)

if len(mes)==1:
    mes="0"+mes

if len(dia)==1:
    dia="0"+dia


def robo_numeraca():

   
    z = zipfile.ZipFile("C:/Users/"+usuario+"/Downloads/isinp.zip","r")
    z.extractall(path="C:/Users/"+usuario+"/Documents/Python Scripts/")
    
    ##
    # Ler arquivos TXT
    ##
    import pandas as pd
    
    arquivo_numeraca="C:/Users/"+usuario+"/Documents/Python Scripts/NUMERACA.TXT"
    arquivo_emissor="C:/Users/"+usuario+"/Documents/Python Scripts/EMISSOR.TXT"
    
    dados_numeraca = pd.read_csv(arquivo_numeraca, header=None, encoding ="iso-8859-1")
    dados_emissor = pd.read_csv(arquivo_emissor, header=None, encoding="iso-8859-1")
    
    ##
    # Colocar nomes nas colunas
    ##
    
    dados_numeraca.columns =[
      "data_geracao_arquivo",
      "acao_sofrida",
      "codigo_isin",
      "codigo_emissor",
      "codigo_cfi",
      "descricao",
      "ano_emissao",
      "data_emissao",
      "ano_expiracao",
      "data_expiracao",
      "taxa_juros",
      "moedas",
      "valor_nominal",
      "preco_exercicio",
      "indexador",
      "percentual_indexador",
      "data_da_acao",
      "codigo_cetip",
      "codigo_selic",
      "codigo_pais",
      "tipo_ativo",
      "codigo_categoria",
      "codigo_especie",
      "data_base",
      "numero_emissao",
      "numero_serie",
      "tipo_emissao",
      "tipo_ativo_objeto",
      "tipo_de_entrega",
      "tipo_de_fundo",
      "tipo_de_garantia",
      "tipo_de_juros",
      "tipo_de_mercado",
      "tipo_status_isin",
      "tipo_vencimento",
      "tipo_protecao",
      "tipo_politica_distribuicao_fundos",
      "tipo_ativos_investidos",
      "tipo_forma",
      "tipo_estilo_opcao",
      "numero_serie_opcao",
      "cod_frequencia_juros",
      "situacao_isin",
      "data_primeiro_pagamento_juros"
    ]
    
    
    dados_emissor.columns = [
    "codigo_emissor",
    "nome_emissor",
    "cnpj_emissor",
    "data_criacao_emissor"
    ]
    
    
    ##
    # Substituis nan por None
    ##
     
    
    dados_numeraca=dados_numeraca.where((pd.notnull(dados_numeraca)), None)
    dados_emissor=dados_emissor.where((pd.notnull(dados_emissor)), None)
    ## Criar coluna com data_bd para a data de inserção no BD
    horario_bd=datetime.datetime.now()
    dados_numeraca["data_bd"]=horario_bd
    dados_emissor["data_bd"]=horario_bd
    
    #dados_emissor.to_excel('C:/Users/Cora.Santos/Desktop/HDI-Investimentos/Base de Dados/dados_emissor.xlsx')
    
    pd.io.sql.to_sql(dados_numeraca, name='bmf_numeraca', con=connection,if_exists="append", flavor='mysql', index=0)
    pd.io.sql.to_sql(dados_emissor, name='bmf_emissor', con=connection,if_exists="append", flavor='mysql', index=0)
    
    # Apagar downloads
    z.close()
    
    import os
    
    os.remove("C:/Users/"+usuario+"/Downloads/isinp.zip")
    os.remove("C:/Users/"+usuario+"/Documents/Python Scripts/NUMERACA.TXT")
    os.remove("C:/Users/"+usuario+"/Documents/Python Scripts/EMISSOR.TXT")
    
robo_numeraca()