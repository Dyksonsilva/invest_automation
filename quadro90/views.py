#Importa library de log
import logging

# Imports do Django
from django.shortcuts import render
import logging
from generator.models import ExecutionDashboard, ExecutionLog, ReportTypes

# Imports de Scripts para atualizações Diárias
from .scripts.xml_parser.xml_parser import *

# Imports funções auxiliares
from dependencias.Metodos.funcoes_auxiliares import *

# Imports scripts para quadro 90
from .scripts.quadro90_228.quadro_90 import quadro90
from quadro90.scripts.xml_quadro_operacoes_org.xml_quadro_operacoes_org import xml_quadro_operacoes_org

def dashboard(request):

    # Variável que condição de where par query relativo ao tipo de atualização no banco
    report_id = 3
    # Variável de controle para loading da página
    control_status = 0
    # Pega o tipo de relatório do banco
    tipo_relatorio = " > " + ReportTypes.objects.get(id=report_id).nome_report

    if "parser" in request.POST:

        start_time = datetime.datetime.now()

        # Parsea XMLs
        xml_parser()
        #print("BMF Preços Futuros OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=20).save()
        control_status = 1

    if "quadro_operacoes" in request.POST:

        start_time = datetime.datetime.now()

        xml_quadro_operacoes_org()
        print("quadro_operacoes")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=21).save()
        control_status = 1

    if "quadro_operacoes_consolidado" in request.POST:

        start_time = datetime.datetime.now()

        logger = logging.getLevelName(__name__)

        array_data = get_data_ultimo_dia_util_mes_anterior()
        array_data = array_data[0]+'-'+array_data[1]+'-'+array_data[2]
        #array_data = '2016-12-30'

        quadro90(array_data, get_global_var('cnpj_hdi'))
        #logger.info("Sucesso na execução do Quadro 90 para HDI")

        #quadro90(array_data, get_global_var('cnpj_gerling'))
        #logger.info('Sucesso na execução do Quadro 90 para Gerling')
        print("quadro_operacoes_consolidado")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=22).save()
        control_status = 1

    return render(request, 'quadro90/dashboard.html', {'ExecutionDashboard': ExecutionDashboard.objects.raw('SELECT exec_dash.*, exec_log.end_time, round(exec_log.end_time - exec_log.start_time) AS tempo_execucao FROM generator_executiondashboard AS exec_dash LEFT JOIN ( SELECT MAX(id) AS id, MAX(start_time) AS start_time, MAX(end_time) AS end_time, MAX(execution_id) AS execution_id  FROM generator_executionlog  GROUP BY execution_id ) AS exec_log ON exec_dash.id = exec_log.execution_id WHERE report_type_id = ' + str(report_id)), 'control_status' : control_status, 'tipo_relatorio' : tipo_relatorio} )