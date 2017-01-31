# Imports
from django.shortcuts import render
import logging

# Imports scripts para VaR
from var.scripts.relatorio_encadeado.processo_quaid_expo_var.processo_quaid_expo_var import processo_quaid_expo_var
from var.scripts.matriz_gerencial.matriz_gerencial_1 import matriz_gerencial_1
from var.scripts.matriz_gerencial.matriz_gerencial_2 import matriz_gerencial_2
from var.scripts.simulacao_credito.simulacao_credito import simulacao_credito
from var.scripts.relatorio_risco_credito.relatorio_risco_credito import relatorio_risco_credito

logger = logging.getLogger(__name__)

def dashboard_escolha(request):

    if "simulacao_credito" in request.POST:

        simulacao_credito()
        print("Simulacao Credito  OK!")

    if "matriz_gerencial" in request.POST:

        retornos = matriz_gerencial_1()
        print("Matriz gerencial 1  OK!")

        input('Aperte Enter depois de executar os procedimentos manuais')

        matriz_gerencial_2(retornos)
        print("Matriz gerencial 2 OK!")

    if "re_expo_var" in request.POST:

        processo_quaid_expo_var()
        print("RE  expo_var OK!")

    if "re_relatorio" in request.POST:

        relatorio_risco_credito()
        print("RE Relatório Gerado OK!")

    return render(request, 'var/dashboard_escolha.html')