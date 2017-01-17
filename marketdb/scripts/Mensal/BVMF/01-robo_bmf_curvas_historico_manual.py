# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 22:29:24 2016

@author: Cora.Santos
"""

# -*- coding: utf-80 -*-
"""
Created on Thu Mar 10 14:38:00 2016

@author: William.Loureiro
"""

import pandas as pd
import xmltodict
import glob
import pymysql as db
import numpy as np
import urllib.request
import urllib #esse
import bizdays
import datetime 
from datetime import date
from selenium import webdriver 
import selenium
import os #esse
import zipfile, time #esse
import io #esse
import html5lib
import warnings    #esse
from xml.dom import minidom #esse
import math #esse
import sys #esse
from pandas.tseries.offsets import *#esse
from findt import FinDt
from bs4 import BeautifulSoup

warnings.simplefilter(action = "ignore")

exec(open("diretorios.py").read())

#import urllib

# proxy_acesso = "http://"+usuario+":"+senha+"@brweb.sa.ey.net:8080"
# proxies = {'http': proxy_acesso}

# proxy = urllib.request.ProxyHandler(proxies)
# opener = urllib.request.build_opener(proxy)
# urllib.request.install_opener(opener)

# connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv')

###############################################################
# 
#       Separar o dia a ser buscado (sempre dia anterior)
#
###############################################################

#hoje = datetime.datetime.now()

# Calendário Anbima
#calendario_anbima=pd.read_excel(var_path_xls)
#feriados=calendario_anbima['Data'][0:936]
#cal=bizdays.Calendar(holidays=feriados, startdate='2000-01-01', weekdays=['sunday', 'saturday'])


dt_ref = pd.date_range(start=datetime.date(int(dtbase[0:4]),int(dtbase[4:6]),1), end=datetime.date(int(dtbase[0:4]),int(dtbase[4:6]),int(dtbase[6:8])), freq='D').date


for i in dt_ref:
    dia=str(i.day)
    mes=str(i.month)
    ano=str(i.year)
    
    if len(mes)==1:
        mes="0"+mes
    
    if len(dia)==1:
        dia="0"+dia

    try:
        robo_bmf_curvas(ano, mes, dia)
        print('du: ',ano,mes,dia)

    except:
        robo_bmf_curvas(ano, mes, dia)
        print('dnu: ',ano,mes,dia)














