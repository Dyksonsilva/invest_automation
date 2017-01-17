##Import libs:
import warnings
import bizdays
import datetime
import importlib
import pandas as pd
import pymysql as db

#Import Scripts:
importlib.import_module(name='01-robo_multiplo_diário_robo_precos_futuros', package=None)
importlib.import_module(name='02-robo_multiplo_diário_robo_anbima_titpublico', package=None)
importlib.import_module(name='03-robo_multiplo_diário_robo_anbima_vna', package=None)
importlib.import_module(name='04-robo_multiplo_diário_robo_anbima_debentures', package=None)
importlib.import_module(name='05-robo_multiplo_diário_robo_anbima_curvas', package=None)
importlib.import_module(name='06-robo_multiplo_diário_robo_anbima_carteiras', package=None)
#importlib.import_module(name='07-robo_multiplo_diário_robos_anbima_projecoes', package=None)

#from 01-robo_multiplo_diário_robo_precos_futuros import robo_precos_futuros
#from 02-robo_multiplo_diário_robo_anbima_titpublico import robo_anbima_titpublico
#from 03-robo_multiplo_diário_robo_anbima_vna import robo_anbima_vna
#from 04-robo_multiplo_diário_robo_anbima_debentures import robo_anbima_debentures
#from 05-robo_multiplo_diário_robo_anbima_curvas import robo_anbima_curvas
#from 06-robo_multiplo_diário_robo_anbima_carteiras import robo_anbima_carteiras
#from 07- robo_multiplo_diário_robos_anbima_projecoes import robos_anbima_projecoes

exec(open("robo_multiplo_diário_definitions.py").read())

#Call Functions:
robo_precos_futuros(ano, mes, dia)
robo_anbima_titpublico(ano,mes,dia)

if(fator_diautil!=1):
    robo_anbima_vna()

robo_anbima_debentures(ano, mes, dia)
robo_anbima_curvas()
robo_anbima_carteiras()
#robos_anbima_projecoes()

horario_fim = datetime.datetime.now()
tempo=horario_fim-horario_inicio
print(tempo)