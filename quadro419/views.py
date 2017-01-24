# Imports do Django
from django.shortcuts import render

# Imports de Scripts para Cálculos do Quadro 419
from .scripts.fluxo_titpublico import *
from .scripts.mtm_titpublico import *
from .scripts.anbima_debentures_ajustes_cadastro import *

def dashboard(request):

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

    return render(request, 'quadro419/dashboard.html')
