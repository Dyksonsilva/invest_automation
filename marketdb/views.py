from django.shortcuts import render
from .scripts.Diaria.Robo_Multiplo_Diario.calculo_data_atual import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_bmf_precos_futuros import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_titpublico import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_vna import *
from .scripts.Diaria.Robo_Multiplo_Diario.get_anbima_debentures import *

#import logging

# Create your views here.

# Get an instance of a logger
#logger = logging.getLogger(__name__)

def dashboard(request):

    if "bmf_precos_futuros" in request.POST:
        # retorna array de datas bases atualizadas: dia, mes e ano

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        # Pega os preços futuros do site da BMF
        get_bmf_precos_futuros(array_data[0], array_data[1], array_data[2])
        print("Preços Futuros BMF OK!" + str(array_data))

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
            get_anbima_vna(array_data[0], array_data[1], array_data[2])
            print("Preços Valor Nominal Atualizado - Anbima OK!" + str(array_data))
        else:
            print("Fator diário igual a 1, não necessário atualização de base")

    if "anbima_debentures" in request.POST:

        array_data = calculo_data_atual()
        print("Array de datas ok!" + str(array_data))

        get_anbima_debentures(array_data[0], array_data[1], array_data[2])
        print("Anbima Debentures OK!" + str(array_data))

    #if "anbima_curvas" in request.POST:

    # array_data = calculo_data_atual()
    # print("Array de datas ok!" + str(array_data))

    # get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
    # print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    #if "anbima_carteiras" in request.POST:

    # array_data = calculo_data_atual()
    # print("Array de datas ok!" + str(array_data))

    # get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
    # print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    #if "anbima_projecoes" in request.POST:

    # array_data = calculo_data_atual()
    # print("Array de datas ok!" + str(array_data))

    # get_anbima_titpublico(array_data[0], array_data[1], array_data[2])
    # print("Preços Títulos Públicos Anbima OK!" + str(array_data))

    return render(request, 'marketdb/dashboard.html')




