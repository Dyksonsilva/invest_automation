
# Imports do Django
from django.shortcuts import render

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

def dashboard_escolha(request):

    return render(request, 'marketdb/dashboard_escolha.html')

def dashboard_diario(request):

    if "bmf_precos_futuros" in request.POST:

        # Retorna array de datas bases atualizadas: dia, mes e ano
        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        # Pega os preços futuros do site da BMF
        get_bmf_precos_futuros(array_data[0], array_data[1], array_data[2])
        print("BMF Preços Futuros OK!")

    if "anbima_titulos_publicos" in request.POST:

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
        print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    if "anbima_vna" in request.POST:

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        # Verifica se o dia útil é diferente de 1 e roda o método
        if (array_data[3] != 1):
            get_anbima_vna()
            print("Preços Valor Nominal Atualizado - Anbima OK!" + str(array_data))
        else:
            print("Fator diário igual a 1, não necessário atualização de base")

    if "anbima_debentures" in request.POST:

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_debentures(array_data[0], array_data[1], array_data[2])
        print("Anbima Debentures OK!" + str(array_data))

    if "anbima_curvas" in request.POST:

        get_anbima_curvas()
        print("Anbima Curvas OK!")

    if "anbima_carteiras" in request.POST:

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_carteiras(array_data[0], array_data[1], array_data[2])
        print("Anbima Carteiras OK!")

    #if "anbima_projecoes" in request.POST:

    # array_data = calculo_data_atual()
    # print("Array de datas ok!" + str(array_data))

    # get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
    # print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    return render(request, 'marketdb/dashboard_diario.html')

def dashboard_mensal(request):

    if "bmf_numeraca" in request.POST:

        # Extrai os ISIN codes do site da BMF e armazena no banco de dados
        get_bmf_numeraca()
        print("BMF Numeraca OK!")

    if "bmf_cotacoes_historicas" in request.POST:

        # Atualiza as cotações históricas de BMF no banco
        get_bmf_cotacoes_hist()
        print("BMF Cotações Históricas OK!")

    return render(request, 'marketdb/dashboard_mensal.html')



