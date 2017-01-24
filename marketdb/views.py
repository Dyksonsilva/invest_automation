# Imports do Django
from django.shortcuts import render
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

#@login_required(login_url="/login/")
def dashboard_mensal(request):

    if "bmf_numeraca" in request.POST:

        # Extrai os ISIN codes do site da BMF e armazena no banco de dados
        get_bmf_numeraca()
        print("BMF Numeraca OK!")

    if "bmf_cotacoes_historicas" in request.POST:

        # Atualiza as cotações históricas de BMF no banco
        get_bmf_cotacoes_hist()
        print("BMF Cotações Históricas OK!")

    if "anbima_debentures" in request.POST:

        # Atualiza Debentures do Site da Anbima no banco de dados
        get_debentures_caracteristicas_hist()
        print("Anbima Debentures OK!")

    if "bmf_curvas_historicas" in request.POST:

        # Atualiza as curvas do
        get_bmf_curvas_historico()
        print("BMF Curvas Históricas OK!")

    if "bacen_series_historicas" in request.POST:

        # Atualiza Séries históricas do Bacen
        get_bacen_series_hist()
        print("BACEN Séries Históricas OK!")

    if "bacen_series_indices" in request.POST:

        # Atualiza Séries históricas do Bacen
        get_bacen_indices()
        print("BACEN Séries Indices OK!")

    if "bloomberg_cotas_valor" in request.POST:

        # Atualiza Cotas do Valor Econômico retirados da Bloomberg
        upload_manual_cotas(
        'SELECT DISTINCT isin, cnpjfundo_outros, cnpjfundo_1nivel from projeto_inv.xml_quadro_operacoes where produto = "fundo"',
        {'cnpj': 'cnpj_fundo'},
        'valoreconomico_cotas')
        print("BLOOMBERG Cotas Valor Econômico OK!")

    if "bloomberg_cotas_fidc" in request.POST:

        # Atualiza Cotas do FIDC retirados da Bloomberg
        upload_manual_cotas(
        'SELECT DISTINCT isin, fundo, cnpjfundo_outros, cnpjfundo_1nivel from projeto_inv.xml_quadro_operacoes where produto = "fundo"',
        {'Cap. Líquida': 'cap_liq', 'Num. Cotistas': 'num_cotista', 'pl': 'patrimonio', 'Var% em 12 meses': 'var_perc_12_meses', 'Var% no ano': 'var_perc_ano', 'Var% no dia': 'var_perc_dia', 'Var% no mês': 'var_perc_mes', 'fundo': 'nome_cota', 'isin_fundo': 'codigo_isin', 'cnpj_fundo': 'cnpj'},
        'fidc_cotas')
        print("BLOOMBERG Cotas FIDC OK!")

    if "bloomberg_importacao_rating" in request.POST:

        # Importação dos ratings da Bloomberg
        importacao_rating_bloomberg()
        print("BLOOMBERG Rating OK!")

    return render(request, 'marketdb/dashboard_mensal.html')



