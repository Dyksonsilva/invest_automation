# Imports comuns
import datetime

# Imports do Django
from django.shortcuts import render
from generator.models import ExecutionDashboard, ExecutionLog, ReportTypes
#from django.contrib.auth.decorators import login_required

# Imports de Scripts para atualizações Diárias
from .scripts.Diaria.Robo_Multiplo_Diario.calculo_data_atual import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_bmf_precos_futuros import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_titpublico import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_vna import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_debentures import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_carteiras import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_curvas import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_projecoes import *

# Imports de Scripts para atualizações Mensais
from .scripts.Mensal.BVMF.get_bmf_numeraca import *
from .scripts.Mensal.BVMF.get_bmf_cotacoes_hist import *
from .scripts.Mensal.ANBIMA.get_debentures_caracteristicas_hist import *
from .scripts.Mensal.BVMF.get_bmf_curvas_historico import *
from .scripts.Mensal.BACEN.get_bacen_series_hist import *
from .scripts.Mensal.BACEN.get_bacen_indices import *
from .scripts.Mensal.BLOOMBERG.upload_manual_cotas import *
from .scripts.Mensal.BLOOMBERG.importacao_rating_bloomberg import *

#@login_required(login_url="/login/")
def dashboard_escolha(request):

    return render(request, 'marketdb/dashboard_escolha.html')

#@login_required(login_url="/login/")
def dashboard_diario(request):

    # Variável que condição de where par query relativo ao tipo de atualização no banco
    report_id = 1
    # Variável de controle para loading da página
    control_status = 0
    # Pega o tipo de relatório do banco
    tipo_relatorio = " > " + ReportTypes.objects.get(id=report_id).nome_report

    if "bmf_precos_futuros" in request.POST:

        start_time = datetime.datetime.now()

        # Retorna array de datas bases atualizadas: dia, mes e ano
        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        # Pega os preços futuros do site da BMF
        get_bmf_precos_futuros(array_data[0], array_data[1], array_data[2])
        print("BMF Preços Futuros OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=1).save()
        control_status = 1

    if "anbima_titulos_publicos" in request.POST:
        start_time = datetime.datetime.now()

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
        print("Preços Títulos Públicos Anbima OK!" + str(array_data))

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=2).save()

    if "anbima_vna" in request.POST:
        start_time = datetime.datetime.now()

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        # Verifica se o dia útil é diferente de 1 e roda o método
        if (array_data[3] != 1):
            get_anbima_vna()
            print("Preços Valor Nominal Atualizado - Anbima OK!" + str(array_data))
        else:
            print("Fator diário igual a 1, não necessário atualização de base")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=3).save()
        control_status = 1

    if "anbima_debentures" in request.POST:
        start_time = datetime.datetime.now()

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_debentures(array_data[0], array_data[1], array_data[2])
        print("Anbima Debentures OK!" + str(array_data))

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=4).save()
        control_status = 1

    if "anbima_curvas" in request.POST:
        start_time = datetime.datetime.now()

        get_anbima_curvas()
        print("Anbima Curvas OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=5).save()
        control_status = 1

    if "anbima_carteiras" in request.POST:
        start_time = datetime.datetime.now()

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_carteiras(array_data[0], array_data[1], array_data[2])
        print("Anbima Carteiras OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=6).save()
        control_status = 1

    #if "anbima_projecoes" in request.POST:

    # array_data = calculo_data_atual()
    # print("Array de datas ok!" + str(array_data))

    # get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
    # print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    return render(request, 'marketdb/dashboard_diario.html', {'ExecutionDashboard': ExecutionDashboard.objects.raw('SELECT exec_dash.*, exec_log.end_time, round(exec_log.end_time - exec_log.start_time) AS tempo_execucao FROM generator_executiondashboard AS exec_dash LEFT JOIN ( SELECT MAX(id) AS id, MAX(start_time) AS start_time, MAX(end_time) AS end_time, MAX(execution_id) AS execution_id  FROM generator_executionlog  GROUP BY execution_id ) AS exec_log ON exec_dash.id = exec_log.execution_id WHERE report_type_id = ' + str(report_id)), 'control_status' : control_status, 'tipo_relatorio' : tipo_relatorio})

#@login_required(login_url="/login/")
def dashboard_mensal(request):

    # Variável que condição de where par query relativo ao tipo de atualização no banco
    report_id = 2
    # Variável de controle para loading da página
    control_status = 0
    # Pega o tipo de relatório do banco
    tipo_relatorio = " > " + ReportTypes.objects.get(id=report_id).nome_report

    if "bmf_numeraca" in request.POST:
        start_time = datetime.datetime.now()

        # Extrai os ISIN codes do site da BMF e armazena no banco de dados
        get_bmf_numeraca()
        print("BMF Numeraca OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=8).save()
        control_status = 1

    if "bmf_cotacoes_historicas" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza as cotações históricas de BMF no banco
        get_bmf_cotacoes_hist()
        print("BMF Cotações Históricas OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=9).save()
        control_status = 1

    if "anbima_debentures" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza Debentures do Site da Anbima no banco de dados
        get_debentures_caracteristicas_hist()
        print("Anbima Debentures OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=10).save()
        control_status = 1

    if "bmf_curvas_historicas" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza as curvas do
        get_bmf_curvas_historico()
        print("BMF Curvas Históricas OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=11).save()
        control_status = 1

    if "bacen_series_historicas" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza Séries históricas do Bacen
        get_bacen_series_hist()
        print("BACEN Séries Históricas OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=12).save()
        control_status = 1

    if "bacen_series_indices" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza Séries históricas do Bacen
        get_bacen_indices()
        print("BACEN Séries Indices OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=13).save()
        control_status = 1

    if "bloomberg_cotas_valor" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza Cotas do Valor Econômico retirados da Bloomberg
        upload_manual_cotas(
        'SELECT DISTINCT isin, cnpjfundo_outros, cnpjfundo_1nivel from projeto_inv.xml_quadro_operacoes where produto = "fundo"',
        {'cnpj': 'cnpj_fundo'},
        'valoreconomico_cotas')
        print("BLOOMBERG Cotas Valor Econômico OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=14).save()
        control_status = 1

    if "bloomberg_cotas_fidc" in request.POST:
        start_time = datetime.datetime.now()

        # Atualiza Cotas do FIDC retirados da Bloomberg
        upload_manual_cotas(
        'SELECT DISTINCT isin, fundo, cnpjfundo_outros, cnpjfundo_1nivel from projeto_inv.xml_quadro_operacoes where produto = "fundo"',
        {'Cap. Líquida': 'cap_liq', 'Num. Cotistas': 'num_cotista', 'pl': 'patrimonio', 'Var% em 12 meses': 'var_perc_12_meses', 'Var% no ano': 'var_perc_ano', 'Var% no dia': 'var_perc_dia', 'Var% no mês': 'var_perc_mes', 'fundo': 'nome_cota', 'isin_fundo': 'codigo_isin', 'cnpj_fundo': 'cnpj'},
        'fidc_cotas')
        print("BLOOMBERG Cotas FIDC OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=15).save()
        control_status = 1

    if "bloomberg_importacao_rating" in request.POST:
        start_time = datetime.datetime.now()

        # Importação dos ratings da Bloomberg
        importacao_rating_bloomberg()
        print("BLOOMBERG Rating OK!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=16).save()
        control_status = 1

    return render(request, 'marketdb/dashboard_mensal.html', {'ExecutionDashboard': ExecutionDashboard.objects.raw('SELECT exec_dash.*, exec_log.end_time, round(exec_log.end_time - exec_log.start_time) AS tempo_execucao FROM generator_executiondashboard AS exec_dash LEFT JOIN ( SELECT MAX(id) AS id, MAX(start_time) AS start_time, MAX(end_time) AS end_time, MAX(execution_id) AS execution_id  FROM generator_executionlog  GROUP BY execution_id ) AS exec_log ON exec_dash.id = exec_log.execution_id WHERE report_type_id = ' + str(report_id)), 'control_status' : control_status, 'tipo_relatorio' : tipo_relatorio})



