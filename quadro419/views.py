# Imports do Django
from django.shortcuts import render
from generator.models import ExecutionDashboard

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

    dict_dashboard = {'ExecutionDashboard': ExecutionDashboard.objects.get(id=1)}

    if "fluxo_titpublicos" in request.POST:
        # Atualiza Fluxo de Títulos Públicos
        fluxo_titpublico()
        print("Fluxo de Títulos Públicos calculado com sucesso!")

    if "mtm_titpublicos" in request.POST:
        # Atualiza o fluxo Market to Market dos títulos públicos
        mtm_titpublico()
        print("Market to Market de Títulos Públicos calculado com sucesso!")

    if 'anbima_debentures_ajustes_cadastro' in request.POST:
        # Atualiza
        anbima_debentures_ajustes_cadastro()
        print("Atualização dos cadastros da Anbima com sucesso!")

    if 'bmf_numeraca_ajustes_cadastro' in request.POST:
        # Atualiza os cadastros do BMF Numeraca
        bmf_numeraca_ajustes_cadastro()
        print("Atualização dos cadastros do BMF Numeraca efetuado com sucesso!")

    if 'fluxo_debentures' in request.POST:
        # Atualiza os Fluxos de Debentures
        fluxo_debentures()
        print("Atualização dos Fluxos de Debentures com sucesso!")

    if 'fluxo_titprivado' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        fluxo_titprivado()
        print("Atualização dos Fluxos de Títulos Privados com sucesso!")

    if 'interpolacao_curvas_bmf' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        interpolacao_curvas_bmf()
        print("Atualização dos interpolacao_curvas_bmf com sucesso!")

    if 'mtm_titulo_debenture' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        mtm_titulo_debenture()
        print("Atualização dos mtm_titulo_debenture com sucesso!")

    if 'mtm_curva_titiprivado' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        mtm_curva_titprivado()
        print("Atualização dos mtm_curva_titiprivado com sucesso!")

    if 'mtm_curva_debenture' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        mtm_curva_debenture()
        print("Atualização dos mtm_curva_debenture com sucesso!")

    if 'finalizacao_fidc' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        finalizacao_fidc()
        print("Atualização dos finalizacao_fidc com sucesso!")

    if 'tabelas_xml_final' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        tabelas_xml_final()
        print("Atualização dos tabelas_xml_final com sucesso!")

    if 'titpublico_final_pv_ettj' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        titpublico_final_pv_ettj()
        print("Atualização dos titpublico_final_pv_ettj com sucesso!")

    if 'xml_quadro_operacoes_hdi' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        xml_quadro_operacoes('29980158000157')
        print("Atualização dos xml_quadro_operacoes_hdi com sucesso!")

    if 'xml_quadro_operacoes_gerling' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        xml_quadro_operacoes('18096627000153')
        print("Atualização dos xml_quadro_operacoes_gerling com sucesso!")

    if 'quadro_operacoes_hdi' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        quadro_operacoes('hdi')
        print("Atualização dos quadro_operacoes_hdi com sucesso!")

    if 'quadro_operacoes_gerling' in request.POST:
        # Atualiza os Fluxos de Títulos Privados
        quadro_operacoes('gerling')
        print("Atualização dos quadro_operacoes_hdi com sucesso!")

    return render(request, 'quadro419/dashboard.html', dict_dashboard)
