# Imports comuns
import logging
import datetime

# Imports do Django
from django.shortcuts import render
from generator.models import ExecutionDashboard, ExecutionLog, ReportTypes

# Imports scripts para VaR
from var.scripts.relatorio_encadeado.processo_quaid_expo_var.processo_quaid_expo_var import processo_quaid_expo_var
from var.scripts.matriz_gerencial.matriz_gerencial_1 import matriz_gerencial_1
from var.scripts.matriz_gerencial.matriz_gerencial_2 import matriz_gerencial_2
from var.scripts.simulacao_credito.simulacao_credito import simulacao_credito
from var.scripts.relatorio_risco_credito.relatorio_risco_credito import relatorio_risco_credito

logger = logging.getLogger(__name__)

def dashboard(request):

    # Variável que condição de where par query relativo ao tipo de atualização no banco
    report_id = 5
    # Variável de controle para loading da página
    control_status = 0
    # Pega o tipo de relatório do banco
    tipo_relatorio = " > " + ReportTypes.objects.get(id=report_id).nome_report

    if "simulacao_credito" in request.POST:

        start_time = datetime.datetime.now()

        simulacao_credito()
        print("Simulacao Credito  OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=39).save()
        control_status = 1

    if "matriz_gerencial" in request.POST:

        start_time = datetime.datetime.now()

        retornos = matriz_gerencial_1()
        print("Matriz gerencial 1  OK!")

        input('Aperte Enter depois de executar os procedimentos manuais')

        matriz_gerencial_2(retornos)
        print("Matriz gerencial 2 OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=40).save()
        control_status = 1


    if "re_expo_var" in request.POST:
        start_time = datetime.datetime.now()

        processo_quaid_expo_var()
        print("RE  expo_var OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=41).save()
        control_status = 1


    if "re_relatorio" in request.POST:
        start_time = datetime.datetime.now()

        relatorio_risco_credito()
        print("RE Relatório Gerado OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=42).save()
        control_status = 1


    return render(request, 'var/dashboard.html', {'ExecutionDashboard': ExecutionDashboard.objects.raw('SELECT exec_dash.*, exec_log.end_time, round(exec_log.end_time - exec_log.start_time) AS tempo_execucao FROM generator_executiondashboard AS exec_dash LEFT JOIN ( SELECT MAX(id) AS id, MAX(start_time) AS start_time, MAX(end_time) AS end_time, MAX(execution_id) AS execution_id  FROM generator_executionlog  GROUP BY execution_id ) AS exec_log ON exec_dash.id = exec_log.execution_id WHERE report_type_id = ' + str(report_id)), 'control_status' : control_status, 'tipo_relatorio': tipo_relatorio})