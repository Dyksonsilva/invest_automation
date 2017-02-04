import datetime

# Imports do Django
from django.shortcuts import render
from generator.models import ExecutionDashboard, ExecutionLog, ReportTypes

# Imports de Scripts para Cálculos do Quadro 419
from .scripts.fluxo_titpublico import *
from .scripts.mtm_titpublico import *
from .scripts.anbima_debentures_ajustes_cadastro import *
from .scripts.bmf_numeraca_ajustes_cadastro import *
from .scripts.fluxo_debentures import *
from .scripts.fluxo_titprivado import *
from .scripts.interpolacao_curvas_bmf import *
from .scripts.mtm_titulo_debenture import *
from .scripts.mtm_curva_titprivado import *
from .scripts.mtm_curva_debenture import *
from .scripts.finalizacao_fidc import *
from .scripts.tabelas_xml_final import *
from .scripts.titpublico_final_pv_ettj import *
from .scripts.xml_quadro_operacoes import *
from .scripts.quadro_operacoes import *

def dashboard(request):

    # Variável que condição de where par query relativo ao tipo de atualização no banco
    report_id = 4
    # Variável de controle para loading da página
    control_status = 0
    # Pega o tipo de relatório do banco
    tipo_relatorio = " > " + ReportTypes.objects.get(id=report_id).nome_report

    if "fluxo_titpublicos" in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza Fluxo de Títulos Públicos
        fluxo_titpublico()
        print("Fluxo de Títulos Públicos calculado com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=23).save()
        control_status = 1

    if "mtm_titpublicos" in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza o fluxo Market to Market dos títulos públicos
        mtm_titpublico()
        print("Market to Market de Títulos Públicos calculado com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=24).save()
        control_status = 1

    if 'anbima_debentures_ajustes_cadastro' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza
        anbima_debentures_ajustes_cadastro()
        print("Atualização dos cadastros da Anbima com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=25).save()
        control_status = 1

    if 'bmf_numeraca_ajustes_cadastro' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os cadastros do BMF Numeraca
        bmf_numeraca_ajustes_cadastro()
        print("Atualização dos cadastros do BMF Numeraca efetuado com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=26).save()
        control_status = 1

    if 'fluxo_debentures' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Debentures
        fluxo_debentures()
        print("Atualização dos Fluxos de Debentures com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=27).save()
        control_status = 1

    if 'fluxo_titprivado' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        fluxo_titprivado()
        print("Atualização dos Fluxos de Títulos Privados com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=28).save()
        control_status = 1

    if 'interpolacao_curvas_bmf' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        interpolacao_curvas_bmf()
        print("Atualização dos interpolacao_curvas_bmf com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=29).save()
        control_status = 1

    if 'mtm_titulo_debenture' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        mtm_titulo_debenture()
        print("Atualização dos mtm_titulo_debenture com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=30).save()
        control_status = 1

    if 'mtm_curva_titiprivado' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        mtm_curva_titprivado()
        print("Atualização dos mtm_curva_titiprivado com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=31).save()
        control_status = 1

    if 'mtm_curva_debenture' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        mtm_curva_debenture()
        print("Atualização dos mtm_curva_debenture com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=32).save()
        control_status = 1

    if 'finalizacao_fidc' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        finalizacao_fidc()
        print("Atualização dos finalizacao_fidc com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=33).save()
        control_status = 1

    if 'tabelas_xml_final' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        tabelas_xml_final()
        print("Atualização dos tabelas_xml_final com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=34).save()
        control_status = 1

    if 'titpublico_final_pv_ettj' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        titpublico_final_pv_ettj()
        print("Atualização dos titpublico_final_pv_ettj com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=35).save()
        control_status = 1

    if 'xml_quadro_operacoes_hdi' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        xml_quadro_operacoes('29980158000157')
        print("Atualização dos xml_quadro_operacoes_hdi com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=36).save()
        control_status = 1

    if 'xml_quadro_operacoes_gerling' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        xml_quadro_operacoes('18096627000153')
        print("Atualização dos xml_quadro_operacoes_gerling com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=37).save()
        control_status = 1

    if 'quadro_operacoes_hdi' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        quadro_operacoes('hdi')
        print("Atualização dos quadro_operacoes_hdi com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=38).save()
        control_status = 1

    if 'quadro_operacoes_gerling' in request.POST:

        start_time = datetime.datetime.now()

        # Atualiza os Fluxos de Títulos Privados
        quadro_operacoes('gerling')
        print("Atualização dos quadro_operacoes_hdi com sucesso!")

        end_time = datetime.datetime.now()
        ExecutionLog(start_time=start_time, end_time=end_time, execution_id=39).save()
        control_status = 1

    return render(request, 'quadro419/dashboard.html', {'ExecutionDashboard': ExecutionDashboard.objects.raw('SELECT exec_dash.*, exec_log.end_time, round(exec_log.end_time - exec_log.start_time) AS tempo_execucao FROM generator_executiondashboard AS exec_dash LEFT JOIN ( SELECT MAX(id) AS id, MAX(start_time) AS start_time, MAX(end_time) AS end_time, MAX(execution_id) AS execution_id  FROM generator_executionlog  GROUP BY execution_id ) AS exec_log ON exec_dash.id = exec_log.execution_id WHERE report_type_id = ' + str(report_id)), 'control_status' : control_status, 'tipo_relatorio' : tipo_relatorio})
