#Importa library de log
import logging

# Imports do Django
from django.shortcuts import render
import logging

# Imports de Scripts para atualizações Diárias
from .scripts.xml_parser.xml_parser import *

# Imports funções auxiliares
from dependencias.Metodos.funcoes_auxiliares import *

# Imports scripts para quadro 90
from .scripts.quadro90_228.quadro_90 import quadro90
from quadro90.scripts.xml_quadro_operacoes_org.xml_quadro_operacoes_org import xml_quadro_operacoes_org

def dashboard_escolha(request):

    if "parser" in request.POST:

        # Parsea XMLs
        xml_parser()
        #print("BMF Preços Futuros OK!")

    if "quadro_operacoes" in request.POST:

        xml_quadro_operacoes_org()
        print("quadro_operacoes")

    if "quadro_operacoes_consolidado" in request.POST:

        logger = logging.getLevelName(__name__)

        array_data = get_data_ultimo_dia_util_mes_anterior()
        array_data = array_data[0]+'-'+array_data[1]+'-'+array_data[2]
        #array_data = '2016-12-30'

        quadro90(array_data, get_global_var('cnpj_hdi'))
        #logger.info("Sucesso na execução do Quadro 90 para HDI")

        #quadro90(array_data, get_global_var('cnpj_gerling'))
        #logger.info('Sucesso na execução do Quadro 90 para Gerling')
        print("quadro_operacoes_consolidado")

    return render(request, 'quadro90/dashboard_escolha.html')