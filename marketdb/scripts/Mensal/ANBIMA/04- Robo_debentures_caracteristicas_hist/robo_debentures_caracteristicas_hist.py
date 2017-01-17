# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 14:07:58 2016

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

dia=str(hoje.day)
mes=str(hoje.month)
ano=str(hoje.year)

if len(mes)==1:
    mes="0"+mes

if len(dia)==1:
    dia="0"+dia

def robos_debentures_caracteristacas(ano,mes,dia):


    diretorio_salvo='C:/Users/'+usuario+'/Downloads/'
    nome_arquivo_parcial = "Debentures.com.br_Caracteristica_em_"+dia+"-"+mes+"-"+ano+"_as_"
    
    nome_total = glob.glob(diretorio_salvo+nome_arquivo_parcial+"*.xls")
    
    ####
    #
    # Ler o arquivo
    #
    ####
    
    ### Mudança de extensão para corrigir BOF/arquivo corrompido durante a leitura
    import os
    os.rename(nome_total[0], nome_total[0].replace("xls","csv"))
    
    debentures_caracteristicas = pd.read_csv(nome_total[0].replace("xls","csv"), skiprows=2, sep ="\t", encoding ="iso-8859-1")
    
    
    # Trocar nan por None
    debentures_caracteristicas = debentures_caracteristicas.where((pd.notnull(debentures_caracteristicas)), None)
    
    #Converter formato de data (olhar depois se necessário)
    #debentures_caracteristicas["Data de Registro CVM da Emissão"]=pd.to_datetime(debentures_caracteristicas["Data de Registro CVM da Emissão"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data de Emissão"]=pd.to_datetime(debentures_caracteristicas["Data de Emissão"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data de Vencimento"]=pd.to_datetime(debentures_caracteristicas["Data de Vencimento"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data de Registro CVM do Programa"]=pd.to_datetime(debentures_caracteristicas["Data de Registro CVM do Programa"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data de Saída / Novo Vencimento"]=pd.to_datetime(debentures_caracteristicas["Data de Saída / Novo Vencimento"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data do Início da Distribuição"]=pd.to_datetime(debentures_caracteristicas["Data do Início da Distribuição"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data da Próxima Repactuação"]=pd.to_datetime(debentures_caracteristicas["Data da Próxima Repactuação"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data do Ato (1)"]=pd.to_datetime(debentures_caracteristicas["Data do Ato (1)"], format="%d/%m/%Y")
    #debentures_caracteristicas["Data do Ato (2)"]=pd.to_datetime(debentures_caracteristicas["Data do Ato (2)"], format="%d/%m/%Y")
    
    # Substituir nomes das colunas
    
    debentures_caracteristicas.columns=[
                                        "codigo_do_ativo",
                                        "empresa",
                                        "serie",
                                        "emissao",
                                        "ipO",
                                        "situacao",
                                        "isin",
                                        "registro_cvm_da_emissao",
                                        "data_de_registro_cvm_da_emissao",
                                        "registro_cvm_do_programa",
                                        "data_de_registro_cvm_do_programa",
                                        "data_de_emissao",
                                        "data_de_vencimento",
                                        "motivo_de_saida_",
                                        "data_de_saida_novo_vencimento",
                                        "data_do_inicio_da_rentabilidade",
                                        "data_do_inicio_da_distribuicao",
                                        "data_da_proxima_repactuacao",
                                        "ato_societario_1",
                                        "data_do_ato_1",
                                        "ato_societario_2",
                                        "data_do_ato_2",
                                        "forma",
                                        "garantia_especie",
                                        "classe",
                                        "quantidade_emitida",
                                        "artigo_14",
                                        "artigo_24",
                                        "quantidade_em_mercado",
                                        "quantidade_em_tesouraria",
                                        "quantidade_resgatada",
                                        "quantidade_cancelada",
                                        "quantidade_convertida_no_snd",
                                        "quantidade_convertida_fora_do_snd",
                                        "quantidade_permutada_no_snd",
                                        "quantidade_permutada_fora_do_snd",
                                        "unidade_monetaria_1",
                                        "valor_nominal_na_emissao",
                                        "unidade_monetaria_2",
                                        "valor_nominal_atual",
                                        "data_ult_vna",
                                        "indice",
                                        "tipo",
                                        "criterio_de_calculo",
                                        "dia_de_referencia_para_indice_de_precos",
                                        "criterio_para_indice",
                                        "corrige_a_cada",
                                        "percentual_multiplicador_rentabilidade",
                                        "limite_da_tjlp",
                                        "tipo_de_tratamento_do_limite_da_tjlp",
                                        "juros_criterio_antigo_do_snd",
                                        "premios_criterio_antigo_do_snd",
                                        "amortizacao_taxa",
                                        "amortizacao_cada",
                                        "amortizacao_unidade",
                                        "amortizacao_carencia",
                                        "amortizacao_criterio",
                                        "tipo_de_amortizacao", #texto muito longo para ser armazenado
                                        "juros_criterio_novo_taxa",
                                        "juros_criterio_novo_prazo",
                                        "juros_criterio_novo_cada",
                                        "juros_criterio_novo_unidade",
                                        "juros_criterio_novo_carencia",
                                        "juros_criterio_novo_criterio",
                                        "juros_criterio_novo_tipo",
                                        "premio_criterio_novo_taxa",
                                        "premio_criterio_novo_prazo",
                                        "premio_criterio_novo_cada",
                                        "premio_criterio_novo_unidade",
                                        "premio_criterio_novo_carencia",
                                        "premio_criterio_novo_criterio",
                                        "premio_criterio_novo_tipo",
                                        "participacao_taxa",
                                        "participacao_cada",
                                        "participacao_unidade",
                                        "participacao_carencia",
                                        "participacao_descricao",
                                        "rating_1",
                                        "agencia_classificadora_1",
                                        "rating_2",
                                        "agencia_classificadora_2",
                                        "rating_3",
                                        "agencia_classificadora_3",
                                        "banco_mandatario",
                                        "agente_fiduciario",
                                        "instituicao_depositaria",
                                        "coordenador_lider",
                                        "cnpj",
                                        "deb_incent_lei_12431",
                                        "escritura_padronizada",
                                        "resgate_antecipado"
                                        ]
         
    
    colunas = debentures_caracteristicas.columns
    # Arrumar pontuação
    for coluna in colunas:
        debentures_caracteristicas[coluna]=debentures_caracteristicas[coluna].apply(lambda x: str(x).replace('.', ''))
        debentures_caracteristicas[coluna]=debentures_caracteristicas[coluna].apply(lambda x: str(x).replace(',', '.'))
        
    # Tirar casos onde Empresa = "None"
    
    debentures_caracteristicas= debentures_caracteristicas[debentures_caracteristicas["empresa"]!="None"]
        
    
    # Colocar data de carga no BD
    debentures_caracteristicas["data_bd"] = datetime.datetime.now()
        
        
    #Salvar informação no BD
    import pymysql as db
    connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv',use_unicode=True, charset="utf8")
    pd.io.sql.to_sql(debentures_caracteristicas, name='anbima_debentures_caracteristicas', con=connection,if_exists="append", flavor='mysql', index=0)
    
    
    #Apagar arquivo
    os.remove(nome_total[0].replace("xls","csv"))
    
robos_debentures_caracteristacas(ano,mes,dia)
