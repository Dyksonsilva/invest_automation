# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 15:34:30 2016

@author: projeto_inv.Santos
"""

###############################################################################
# 0 - Declaração de constantes e importação de pacotes
###############################################################################

import datetime, time
import pandas as pd
import numpy as np
import pymysql as db
from pandas import ExcelWriter
from findt import FinDt

exec(open("diretorios.py").read())

horario_inicio = datetime.datetime.now()
tol = 0.20

senhabd = "projetoinvbd"

# Diretório raíz
# root = 'C:/Users/William.Loureiro/Documents/Projeto_Fundos/Exemplo Rotina/codigo_fonte_final'

# Diretório de save de planilhas
save_path = root + '/codigo_fonte_final/Validacoes/'

writer = ExcelWriter(save_path + '/puposicao_final_deb.xlsx')

horario_bd = datetime.datetime.today()

# Para o findt - planilha de feriados nacionais
# var_path= "C:/Users/William.Loureiro/Documents/Projeto_Fundos/Exemplo Rotina/codigo_fonte_final/codigo_fonte_final/feriados_nacionais.csv"


# dt_base_rel_str = '20160831'
dt_base_rel = datetime.date(int(dtbase[0:4]), int(dtbase[4:6]), int(dtbase[6:8]))

'''

                    PREENCHIMENTO TITULO PRIVADO

'''

###############################################################################
# 1 - Leitura e criação de tabelas
###############################################################################

# Informações do cálculo de MTM
connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')

x = 'select * from projeto_inv.mtm_titprivado'

mtm_titprivado0 = pd.read_sql(x, con=connection)

mtm_titprivado = mtm_titprivado0.copy()

# mtm_titprivado = mtm_titprivado.sort(['codigo_isin','id_papel','flag','data_fim','data_bd'],ascending=[True,True,True,True,False])
# mtm_titprivado = mtm_titprivado.drop_duplicates(subset=['codigo_isin','id_papel','flag','taxa_juros','perc_amortizacao','data_fim'],take_last=False)

# Seleciona debentures
mtm_titprivado = mtm_titprivado[mtm_titprivado.tipo_ativo.isin(['DBS'])]

# Seleciona a última carga de debentures da data da posicao
mtm_titprivado['dtrel'] = mtm_titprivado['id_papel'].str.split('_')
mtm_titprivado['dtrel'] = mtm_titprivado['dtrel'].str[0]

mtm_titprivado = mtm_titprivado[mtm_titprivado.dtrel == dtbase].copy()
mtm_titprivado = mtm_titprivado[mtm_titprivado.data_bd == max(mtm_titprivado.data_bd)]

# Renomeia columnas
mtm_titprivado = mtm_titprivado.rename(columns={'data_fim': 'dt_ref', 'dtoperacao': 'dtoperacao_mtm'})

# Reajusta papéis indesaxos a DI
mtm_titprivado['dt_ref'] = pd.to_datetime(mtm_titprivado['dt_ref'])
mtm_titprivado['dt_ref'] = np.where(mtm_titprivado['indexador'] == 'DI1',
                                    mtm_titprivado['dt_ref'] + pd.DateOffset(months=0, days=1),
                                    mtm_titprivado['dt_ref'])
mtm_titprivado['dt_ref'] = mtm_titprivado['dt_ref'].dt.date

# Altera o nome do id_papel para levar em consideração o flag
mtm_titprivado['id_papel_old'] = mtm_titprivado['id_papel']
mtm_titprivado['id_papel'] = mtm_titprivado['id_papel_old'].str.split('_')
mtm_titprivado['id_papel'] = mtm_titprivado['id_papel'].str[0] + '_' + mtm_titprivado['id_papel'].str[1] + '_' + \
                             mtm_titprivado['id_papel'].str[2]

del mtm_titprivado['data_bd']
del mtm_titprivado['dtrel']

connection.close()

# Informações XML
connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')

x = 'select * from projeto_inv.xml_debenture_org'

xml_titprivado = pd.read_sql(x, con=connection)

# Seleciona a última carga de debentures da data da posicao
xml_titprivado['dtrel'] = xml_titprivado['id_papel'].str.split('_')
xml_titprivado['dtrel'] = xml_titprivado['dtrel'].str[0]

xml_titprivado = xml_titprivado[xml_titprivado.dtrel == dtbase].copy()
xml_titprivado = xml_titprivado[xml_titprivado.data_bd == max(xml_titprivado.data_bd)]

original = xml_titprivado.copy()

del xml_titprivado['data_bd']
del xml_titprivado['indexador']
del xml_titprivado['dtrel']

connection.close()

# Puxa as informações de negociação em mercado secuindário da Anbima para debentures -> linha dtspread
x = 'select * from projeto_inv.anbima_debentures'

connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')

anbima_deb = pd.read_sql(x, con=connection)

connection.close()

anbima_deb = anbima_deb[anbima_deb.data_referencia <= dt_base_rel]

anbima_deb = anbima_deb.sort(['codigo', 'data_referencia', 'data_bd'], ascending=[True, True, True])
anbima_deb = anbima_deb.drop_duplicates(subset=['codigo'], take_last=True)

anbima_deb = anbima_deb[['codigo', 'data_referencia', 'pu']].copy()

anbima_deb['codigo'] = anbima_deb['codigo'].astype(str)

anbima_deb = anbima_deb.rename(columns={'codigo': 'coddeb', 'data_referencia': 'data_spread'})

# Criação da tabela xml + anbima
xml_titprivado = xml_titprivado.merge(anbima_deb, on=['coddeb'], how='left')

# Para os papéis que não tiveram negociação, assume data_spread = data_relatorio
xml_titprivado['data_spread'] = np.where(xml_titprivado['data_spread'].isnull(), dt_base_rel,
                                         xml_titprivado['data_spread'])

# Preenchimento com puposicao do xml no pu vindo da anbima quando não tem pu
# Tira o valor médio de todos os pu's posicao
x = xml_titprivado[['isin', 'puposicao']].groupby(['isin']).agg(['mean'])
x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
x1 = pd.DataFrame(columns=['isin', 'pumedio'])
x1['isin'] = x['isin']
x1['pumedio'] = x['puposicao']
xml_titprivado = xml_titprivado.merge(x1, on=['isin'], how='left')

xml_titprivado['pu'] = np.where(xml_titprivado['pu'].isnull(), xml_titprivado['pumedio'], xml_titprivado['pu'])

del xml_titprivado['pumedio']

# Criação da tabela mtm + xml
titprivado = xml_titprivado.merge(mtm_titprivado, on=['id_papel'], how='left')

# Criação da coluna de data de referencia da posição
titprivado['data_referencia'] = titprivado['id_papel'].str[0:8]
titprivado['data_referencia'] = pd.to_datetime(titprivado['data_referencia']).dt.date

###############################################################################
# 2 - Cálculo marcação na curva
###############################################################################

titprivado_curva = titprivado[titprivado.caracteristica == 'V'].copy()

del titprivado_curva['vne']

# Seleciona a parte do fluxo entre a data da compra e a data da posição
titprivado_curva = titprivado_curva[titprivado_curva.dt_ref >= titprivado_curva.dtoperacao_mtm].copy()
titprivado_curva = titprivado_curva[titprivado_curva.dt_ref <= titprivado_curva.data_referencia].copy()

# Preenchimento do VNE na data da compra
tp_curva_dtop = titprivado_curva[['id_papel_old',
                                  'saldo_dev_juros_perc',
                                  'pucompra'
                                  ]][titprivado_curva.dt_ref == titprivado_curva.dtoperacao_mtm].copy()

tp_curva_dtop['vne'] = tp_curva_dtop['pucompra'] * (1 - tp_curva_dtop['saldo_dev_juros_perc'])

del tp_curva_dtop['saldo_dev_juros_perc']
del tp_curva_dtop['pucompra']

titprivado_curva = titprivado_curva.merge(tp_curva_dtop, on=['id_papel_old'], how='left')

titprivado_curva['principal_perc_acum'] = 1 - titprivado_curva['principal_perc']
titprivado_curva['principal_perc_acum'] = titprivado_curva[['id_papel_old', 'principal_perc_acum']].groupby(
    ['id_papel_old']).agg(['cumprod'])

titprivado_curva['vne'] = titprivado_curva['vne'] * titprivado_curva['principal_perc_acum']
titprivado_curva['pagto_juros'] = titprivado_curva['vne'] * titprivado_curva['pagto_juros_perc']
titprivado_curva['vna'] = titprivado_curva['vne'] * titprivado_curva['fator_index_per']
titprivado_curva['vna'][titprivado_curva.indexador == 'DI1'] = titprivado_curva['vne'][
    titprivado_curva.indexador == 'DI1']
titprivado_curva['saldo_dev_juros'] = titprivado_curva['vna'] * titprivado_curva['saldo_dev_juros_perc']
titprivado_curva['pupar'] = titprivado_curva['vna'] + titprivado_curva['saldo_dev_juros'] + titprivado_curva[
    'pagto_juros']

titprivado_curva['dif_curva'] = titprivado_curva['pupar'] / titprivado_curva['puposicao'] - 1
titprivado_curva['dif_curva'] = titprivado_curva['dif_curva'].abs()

titprivado_curva = titprivado_curva[titprivado_curva.dt_ref == titprivado_curva.data_referencia].copy()

titprivado_curva = titprivado_curva[['id_papel_old', 'id_papel', 'codigo_isin', 'dif_curva', 'pupar']].copy()

titprivado_curva = titprivado_curva.sort(['dif_curva'], ascending=[True])
titprivado_curva = titprivado_curva.drop_duplicates(subset=['id_papel'], take_last=False)

titprivado = titprivado.merge(titprivado_curva, on=['id_papel_old', 'id_papel', 'codigo_isin'], how='left')

titprivado = titprivado[
    ((titprivado.caracteristica == 'V') & (titprivado.dif_curva.notnull())) | (titprivado.caracteristica == 'N')].copy()

titprivado = titprivado.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')

###############################################################################
# 4 - Cálculo do mtm na data_spread; a) prazo_du_spread
###############################################################################

writer = ExcelWriter(save_path + '/puposicao_final_deb.xlsx')

# Verificação de papéis sem a data de referência
titprivado[titprivado.dt_ref.isnull()].to_excel(writer, 'dt_ref_NaN')

# Retira papéis sem data de referência
titprivado = titprivado[titprivado.dt_ref.notnull()].copy()

# Cria vetor das datas
dt_min = min(titprivado['dt_ref'])
dt_min = dt_min.replace(day=1)
dt_min = datetime.date(dt_min.year, dt_min.month, dt_min.day)

dt_max = max(titprivado['dt_ref'])
dt_max = dt_max.replace(day=1, month=dt_max.month)
dt_max = dt_max + pd.DateOffset(months=1)
dt_max = dt_max - pd.DateOffset(days=1)
dt_max = datetime.date(dt_max.year, dt_max.month, dt_max.day)

dt_ref = pd.date_range(start=dt_min, end=dt_max, freq='D').date

serie_dias = pd.DataFrame(columns=['dt_ref', 'aux'])
serie_dias['dt_ref'] = dt_ref

# Cria vetor das datas úteis
per = FinDt.DatasFinanceiras(dt_min, dt_max, path_arquivo=var_path)

du = pd.DataFrame(columns=['dt_ref'])
du['dt_ref'] = per.dias(3)
du['du_1'] = 1

serie_dias = serie_dias.merge(du, on=['dt_ref'], how='left')

serie_dias['du_1'] = serie_dias['du_1'].fillna(0)

serie_dias['indice_du'] = np.cumsum(serie_dias['du_1'])

del serie_dias['aux']
del serie_dias['du_1']

fim = serie_dias.copy()
fim = fim.rename(columns={'indice_du': 'indice_du_fim_spread'})

inicio = serie_dias.copy()
inicio = inicio.rename(columns={'dt_ref': 'data_spread', 'indice_du': 'indice_du_inicio_spread'})

# Une as séries dias à tabela titprivado
titprivado = titprivado.merge(fim, on=['dt_ref'], how='left')
titprivado = titprivado.merge(inicio, on=['data_spread'], how='left')

# Calcula o prazo_du_spread
titprivado['prazo_du_spread'] = titprivado['indice_du_fim_spread'] - titprivado['indice_du_inicio_spread']

###############################################################################
# 4 - Cálculo do mtm na data_spread; b) taxa_spot
###############################################################################

if len(titprivado[titprivado.indexador == 'PRE']) != 0:
    maximo_tp_PRE = max(titprivado['prazo_du_spread'][titprivado.indexador == 'PRE'])

if len(titprivado[titprivado.indexador == 'IGP']) != 0:
    maximo_tp_IGPM = max(titprivado['prazo_du_spread'][titprivado.indexador == 'IGP'])

if len(titprivado[titprivado.indexador == 'IPCA']) != 0:
    maximo_tp_IPCA = max(titprivado['prazo_du_spread'][titprivado.indexador == 'IPCA'])

###############################################################################
# ----Base de interpolações para cálculo do spread
###############################################################################
dt_min_interpol = str(min(titprivado['data_spread']))

connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')
x = 'select * from projeto_inv.curva_ettj_interpol where dt_ref<=' + '"' + dtbase + '" and dt_ref>=' + '"' + dt_min_interpol + '" and indexador_cod in("PRE","DIM","DIC");'
ettj = pd.read_sql(x, con=connection)
# Seleciona a última carga
ettj = ettj.sort(['indexador_cod', 'dt_ref', 'data_bd'], ascending=[True, False, False])
ettj = ettj.drop_duplicates(subset=['prazo', 'tx_spot', 'tx_spot_ano', 'tx_termo_dia', 'indexador_cod'],
                            take_last=False)
ettj['indexador'] = np.where(ettj['indexador_cod'] == 'DIC', 'IPCA',
                             np.where(ettj['indexador_cod'] == 'DIM', 'IGP', 'PRE'))
ettj = ettj.rename(columns={'prazo': 'prazo_du'})
ettj_filtro = ettj[['prazo_du', 'tx_spot', 'tx_spot_ano', 'indexador']]
###############################################################################

ettj_filtro = ettj_filtro.rename(columns={'prazo_du': 'prazo_du_spread'})

# Extrapolação PRE, se necessário
if len(titprivado[titprivado.indexador == 'PRE']) != 0:
    maximo_ettj = max(ettj_filtro['prazo_du_spread'][ettj_filtro.indexador == 'PRE'])
    # del ettj_fluxo
    if maximo_ettj < max(titprivado['prazo_du_spread'][titprivado.indexador == 'PRE']):
        ettj_filtro_PRE = ettj_filtro[['prazo_du_spread', 'tx_spot_ano', 'indexador']][
            ettj_filtro.indexador == 'PRE'].copy()
        ettj_filtro_PRE = ettj_filtro_PRE.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
        ettj_filtro_PRE = ettj_filtro_PRE[0:maximo_ettj - 1].copy()

        tx_max = ettj_filtro_PRE['tx_spot_ano'].loc[len(ettj_filtro_PRE) - 1]

        ettj_aux = pd.DataFrame(columns=['prazo_du_spread', 'indexador'])
        ettj_aux['prazo_du_spread'] = np.linspace(1, maximo_tp_PRE, maximo_tp_PRE)
        ettj_aux['indexador'] = 'PRE'
        ettj_aux = ettj_aux.merge(ettj_filtro_PRE, on=['prazo_du_spread', 'indexador'], how='left')
        ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
        ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du_spread'] / 252) - 1
        ettj_fluxo = ettj_fluxo.append(ettj_aux)
    else:
        ettj_aux = ettj_filtro.copy()
        ettj_fluxo = ettj_aux.copy()
else:
    ettj_fluxo = ettj_filtro.copy()

# Extrapolação IGPM, se necessário
if len(titprivado[titprivado.indexador == 'IGP']) != 0:
    maximo_ettj = max(ettj_filtro['prazo_du_spread'][ettj_filtro.indexador == 'IGP'])
    # del ettj_fluxo
    if maximo_ettj < max(titprivado['prazo_du_spread'][titprivado.indexador == 'IGP']):
        ettj_filtro_IGPM = ettj_filtro[['prazo_du_spread', 'tx_spot_ano', 'indexador']][
            ettj_filtro.indexador == 'IGP'].copy()
        ettj_filtro_IGPM = ettj_filtro_IGPM.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
        ettj_filtro_IGPM = ettj_filtro_IGPM[0:maximo_ettj - 1].copy()

        tx_max = ettj_filtro_IGPM['tx_spot_ano'].loc[len(ettj_filtro_IGPM) - 1]

        ettj_aux = pd.DataFrame(columns=['prazo_du_spread', 'indexador'])
        ettj_aux['prazo_du_spread'] = np.linspace(1, maximo_tp_IGPM, maximo_tp_IGPM)
        ettj_aux['indexador'] = 'IGP'
        ettj_aux = ettj_aux.merge(ettj_filtro_IGPM, on=['prazo_du_spread', 'indexador'], how='left')
        ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
        ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du_spread'] / 252) - 1
        ettj_fluxo = ettj_fluxo.append(ettj_aux)
    else:
        ettj_aux = ettj_filtro.copy()
        ettj_fluxo = ettj_aux.copy()
else:
    ettj_fluxo = ettj_filtro.copy()

# Extrapolação IPCA, se necessário
if len(titprivado[titprivado.indexador == 'IPCA']) != 0:
    maximo_ettj = max(ettj_filtro['prazo_du_spread'][ettj_filtro.indexador == 'IPCA'])
    # del ettj_fluxo
    if maximo_ettj < max(titprivado['prazo_du_spread'][titprivado.indexador == 'IPCA']):
        ettj_filtro_IPCA = ettj_filtro[['prazo_du_spread', 'tx_spot_ano', 'indexador']][
            ettj_filtro.indexador == 'IPCA'].copy()
        ettj_filtro_IPCA = ettj_filtro_IPCA.reset_index(level=None, drop=True, inplace=False, col_level=0, col_fill='')
        ettj_filtro_IPCA = ettj_filtro_IPCA[0:maximo_ettj - 1].copy()

        tx_max = ettj_filtro_IPCA['tx_spot_ano'].loc[len(ettj_filtro_IPCA) - 1]

        ettj_aux = pd.DataFrame(columns=['prazo_du_spread', 'indexador'])
        ettj_aux['prazo_du_spread'] = np.linspace(1, maximo_tp_IPCA, maximo_tp_IPCA)
        ettj_aux['indexador'] = 'IPCA'
        ettj_aux = ettj_aux.merge(ettj_filtro_IPCA, on=['prazo_du_spread', 'indexador'], how='left')
        ettj_aux['tx_spot_ano'] = ettj_aux['tx_spot_ano'].fillna(tx_max)
        ettj_aux['tx_spot'] = (1 + ettj_aux['tx_spot_ano']) ** (ettj_aux['prazo_du_spread'] / 252) - 1
        ettj_fluxo = ettj_fluxo.append(ettj_aux)
    else:
        ettj_aux = ettj_filtro.copy()
        ettj_fluxo = ettj_aux.copy()
else:
    ettj_fluxo = ettj_filtro.copy()

# Une a ETTJ à tabela titprivado
ettj_fluxo = ettj_fluxo.rename(columns={'tx_spot': 'tx_spot_spread', 'tx_spot_ano': 'tx_spot_ano_spread'})
titprivado = titprivado.merge(ettj_fluxo, on=['prazo_du_spread', 'indexador'], how='left')

# Preenche com 0 onde não tem taxa spot (prazo_su_spread<0, indexador=DI1)
titprivado['tx_spot_spread'] = titprivado['tx_spot_spread'].fillna(0)
titprivado['tx_spot_ano_spread'] = titprivado['tx_spot_ano_spread'].fillna(0)

###############################################################################
# 4 - Cálculo do mtm na data_spread; c) valor presente e mtm
###############################################################################

titprivado['fator_desconto_spread'] = 1 / (1 + titprivado['tx_spot_spread'])

titprivado['pv_spread'] = titprivado['fv'] * titprivado['fator_desconto_spread']
titprivado['pv_spread'] = np.where(titprivado['prazo_du_spread'] < 0, 0, titprivado['pv_spread'])

x = titprivado[['id_papel', 'pv_spread']].groupby(['id_papel']).agg(['sum'])
x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
x1 = pd.DataFrame(columns=['id_papel', 'mtm_spread'])
x1['id_papel'] = x['id_papel']
x1['mtm_spread'] = x['pv_spread']
titprivado = titprivado.merge(x1, on=['id_papel'], how='left')

titprivado['dif'] = titprivado['mtm_spread'] / titprivado['pu'] - 1

writer = ExcelWriter(save_path + '/puposicao_final_deb.xlsx')

aux = titprivado.drop_duplicates(subset=['codigo_isin'])
aux[['id_papel_old', 'codigo_isin', 'flag', 'indexador', 'puemissao', 'data_emissao', 'data_expiracao', 'puposicao',
     'percentual_indexador', 'taxa_juros', 'pu', 'mtm_spread', 'dif']].to_excel(writer, 'dif')

###############################################################################
# 4 - Cálculo spread de crédito
###############################################################################

titprivado_spread = \
titprivado[['id_papel', 'codigo_isin', 'data_spread', 'pu', 'dt_ref', 'prazo_du_spread', 'pv_spread']][
    (titprivado.caracteristica == 'N') & (titprivado.dif != 1)]

# Seleciona apenas o fluxo com prazo_du positivo
titprivado_spread = titprivado_spread[titprivado_spread.dt_ref >= titprivado_spread.data_spread]

titprivado_spread = titprivado_spread.drop_duplicates(subset=['id_papel', 'prazo_du_spread'])

titprivado_spread['pv_pv_fluxo'] = np.where(titprivado_spread['dt_ref'] == titprivado_spread['data_spread'],
                                            -titprivado_spread['pu'], titprivado_spread['pv_spread'])

tp_spread = titprivado_spread[['id_papel', 'dt_ref', 'prazo_du_spread', 'pv_pv_fluxo']].copy()
tp_spread['prazo_du'] = tp_spread['prazo_du_spread'].astype(float)

tp_spread = tp_spread.drop_duplicates(subset=['id_papel', 'prazo_du_spread'], take_last=True)

id_papel = titprivado_spread['id_papel'].unique()
spread = np.zeros((len(id_papel)))

spread_aux = pd.DataFrame(columns=['id_papel', 'spread'])
spread_aux['id_papel'] = id_papel

start_time = time.time()
for i in range(0, len(id_papel)):
    v = tp_spread['pv_pv_fluxo'][tp_spread.id_papel == id_papel[i]].values
    v = np.meshgrid(v, sparse=True)

    s = np.linspace(-0.9999, 0.9999, 10000)
    t = tp_spread['prazo_du_spread'][tp_spread.id_papel == id_papel[i]].values
    t, s = np.meshgrid(t, s, sparse=True)
    f_ts = 1. / (1 + s) ** (t / 252)

    f_spread = v * f_ts

    f_sum = f_spread.sum(axis=1, dtype='float')

    min_index = abs(f_sum).argmin()

    spread[i] = s[min_index]

    print(time.time() - start_time, i, id_papel[i], spread[i])

    spread_aux['spread'].iloc[i] = spread[i]

titprivado = titprivado.merge(spread_aux, on=['id_papel'], how='left')

aux = titprivado.drop_duplicates(subset=['id_papel'])

aux[['id_papel_old', 'codigo_isin', 'valor_nominal', 'puposicao', 'mtm_spread', 'pu', 'spread']].to_excel(
    save_path + '/teste_dif_deb.xlsx')

###############################################################################
# 5 - Seleção dos papéis cuja marcação não ficou boa
###############################################################################

tp_bigdif = titprivado[['data_spread',
                        'codigo_isin',
                        'id_papel',
                        'flag',
                        'indexador',
                        'dtemissao',
                        'data_emissao',
                        'dtvencimento',
                        'data_expiracao',
                        'valor_nominal',
                        'puemissao',
                        'juros_cada',
                        'coupom',
                        'taxa_juros',
                        'percentual_indexador',
                        'percindex',
                        'perc_amortizacao',
                        'dt_ref',
                        'vne',
                        'du_per',
                        'prazo_du',
                        'fator_index_per',
                        'fator_juros_per',
                        'pv',
                        'fv',
                        'mtm',
                        'puposicao',
                        'pu',
                        'dif',
                        'spread']].copy()

tp_bigdif['dif'] = tp_bigdif['mtm'] / tp_bigdif['pu'] - 1

tp_bigdif[
    (tp_bigdif.dif > tol) | (tp_bigdif.dif < -tol) | (tp_bigdif.spread > tol) | (tp_bigdif.spread < -tol)].to_excel(
    writer, 'bigdif')

###############################################################################
# 6 - Atualização do fluxo de percentual de mtm com o spread e carregamento da tabela para preenchimento do quadro 419
###############################################################################

titprivado_perc = titprivado.copy()

titprivado_perc = titprivado_perc.rename(
    columns={'mtm': 'mtm_old', 'pv': 'pv_old', 'pv_DV100': 'pv_DV100_old', 'fator_desconto': 'fator_desconto_old',
             'fator_desconto_DV100': 'fator_desconto_DV100_old', 'DV100': 'DV100_old'})

# Escolhe o melhor spread - SIGNIFICA O MELHOR FLUXO
titprivado_perc['spread'] = titprivado_perc['spread'].fillna(0)

# Pega penas uma linha para não ter problemas
x = titprivado_perc[['id_papel', 'codigo_isin', 'spread']][
    titprivado_perc.dt_ref == titprivado_perc.data_referencia].copy()

x = x.sort(['codigo_isin', 'spread'], ascending=[True, True])
x = x.drop_duplicates(subset=['codigo_isin'], take_last=False)

x['marker'] = 1

titprivado_perc = titprivado_perc.merge(x, on=['codigo_isin', 'id_papel', 'spread'], how='left')

titprivado_perc = titprivado_perc[titprivado_perc.marker == 1].copy()
del titprivado_perc['marker']

titprivado_perc = titprivado_perc.drop_duplicates(subset=['codigo_isin', 'dt_ref'], take_last=True)

# titprivado_perc['puposicao_final'] = np.where(titprivado_perc['caracteristica']=='N',titprivado_perc['pu'],titprivado_perc['mtm_old'])

titprivado_perc = titprivado_perc[titprivado_perc.prazo_du >= 0]

aux = titprivado_perc[['id_papel', 'codigo_isin']][titprivado_perc.dt_ref == titprivado_perc.data_referencia].copy()
aux = aux.drop_duplicates(subset=['codigo_isin'], take_last=True)
aux['marker'] = 1

titprivado_perc = titprivado_perc.merge(aux, on=['id_papel', 'codigo_isin'], how='left')
titprivado_perc = titprivado_perc[titprivado_perc.marker == 1].copy()
del titprivado_perc['marker']

# Recalcula apenas para que está marcado a mercado
aux = titprivado_perc[titprivado_perc.caracteristica == 'V'].copy()
aux['mtm_DV100_N'] = 0.0
aux = aux.rename(columns={'mtm_old': 'mtm'})

titprivado_perc = titprivado_perc[titprivado_perc.caracteristica == 'N'].copy()

# Cálculo do fator de desconto atualizado pelo spread
titprivado_perc['fator_desconto'] = titprivado_perc['fator_desconto_old'] / (1 + titprivado_perc['spread']) ** (
titprivado_perc['prazo_du'] / 252)
titprivado_perc['fator_desconto_DV100'] = titprivado_perc['fator_desconto_DV100_old'] / (1 + titprivado_perc[
    'spread']) ** (titprivado_perc['prazo_du'] / 252)

# Calculo do pv
titprivado_perc['pv'] = titprivado_perc['fv'] * titprivado_perc['fator_desconto']
titprivado_perc['pv_DV100'] = titprivado_perc['fv'] * titprivado_perc['fator_desconto_DV100']

# Calculo do MTM
x = titprivado_perc[['codigo_isin', 'pv', 'pv_DV100']].groupby(['codigo_isin']).agg(['sum'])
x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
x1 = pd.DataFrame(columns=['codigo_isin', 'mtm', 'mtm_DV100_N'])
x1['codigo_isin'] = x['codigo_isin']
x1['mtm'] = x['pv']
x1['mtm_DV100_N'] = x['pv_DV100']
titprivado_perc = titprivado_perc.merge(x1, on=['codigo_isin'], how='left')

titprivado_perc['mtm_DV100'] = 0.0

# Escolhe o melhor fluxo
titprivado_perc['dif_new'] = titprivado_perc['mtm'] - titprivado_perc['pu']
titprivado_perc['dif_new'] = titprivado_perc['dif_new'].abs()
titprivado_perc['dif_old'] = titprivado_perc['mtm_old'] - titprivado_perc['pu']
titprivado_perc['dif_old'] = titprivado_perc['dif_old'].abs()

titprivado_perc['mtm'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'], titprivado_perc['mtm_old'],
                                  titprivado_perc['mtm'])
titprivado_perc['mtm_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                        titprivado_perc['mtm_DV100'], titprivado_perc['mtm_DV100_N'])
titprivado_perc['pv'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'], titprivado_perc['pv_old'],
                                 titprivado_perc['pv'])
titprivado_perc['pv_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                       titprivado_perc['pv_DV100_old'], titprivado_perc['pv_DV100'])
titprivado_perc['fator_desconto'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                             titprivado_perc['fator_desconto_old'], titprivado_perc['fator_desconto'])
titprivado_perc['fator_desconto_DV100'] = np.where(titprivado_perc['dif_old'] < titprivado_perc['dif_new'],
                                                   titprivado_perc['fator_desconto_DV100_old'],
                                                   titprivado_perc['fator_desconto_DV100'])

titprivado_perc['dif_perc'] = titprivado_perc['dif_new'] / titprivado_perc['pu']

# titprivado_perc['mtm'] = np.where(titprivado_perc['dif_perc']>0.10,titprivado_perc['pu'],titprivado_perc['mtm'])

# Cálculo do DV100
titprivado_perc = titprivado_perc.append(aux)

titprivado_perc['DV100'] = titprivado_perc['mtm'] - titprivado_perc['mtm_DV100']

# Cálculo do perc_mtm
titprivado_perc['perc_mtm'] = titprivado_perc['pv'] / titprivado_perc['mtm']

# Cálculo da duration
titprivado_perc['duration'] = titprivado_perc['perc_mtm'] * titprivado_perc['prazo_du']
x = titprivado_perc[['codigo_isin', 'duration']].groupby(['codigo_isin']).agg(['sum'])
x = x.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
x1 = pd.DataFrame(columns=['codigo_isin', 'duration'])
x1['codigo_isin'] = x['codigo_isin']
x1['duration'] = x['duration']
del titprivado_perc['duration']
titprivado_perc = titprivado_perc.merge(x1, on=['codigo_isin'], how='left')

titprivado_perc[['codigo_isin', 'pu', 'mtm', 'dt_ref', 'mtm_old', 'dif_new', 'dif_old', 'duration', 'spread']].to_excel(
    writer, 'conclusao_fluxo')
titprivado_perc[titprivado_perc.dt_ref == titprivado_perc.data_referencia][
    ['codigo_isin', 'pu', 'mtm', 'dt_ref', 'mtm_old', 'dif_new', 'dif_old', 'duration', 'spread']].to_excel(writer,
                                                                                                            'conclusao_resumido')

finalizacao = titprivado_perc[['codigo_isin', 'mtm']][titprivado_perc.dt_ref == titprivado_perc.data_referencia].copy()

del titprivado_perc['fator_desconto_DV100_old']
del titprivado_perc['fator_desconto_old']
del titprivado_perc['mtm_old']
del titprivado_perc['mtm_DV100_N']
del titprivado_perc['pv_DV100_old']
del titprivado_perc['pv_old']
del titprivado_perc['DV100_old']
del titprivado_perc['dif_new']
del titprivado_perc['dif_old']
del titprivado_perc['dif_perc']

writer.save()

'''

    Alteração de formato das colunas que são int e foram lidas como float (sabe lá pq...)

'''

# id_mtm_titprivado
titprivado_perc['id_mtm_titprivado'] = titprivado_perc['id_mtm_titprivado'].astype(int)

# id_bmf_numeraca
aux = titprivado_perc[['id_papel', 'id_bmf_numeraca']][titprivado_perc.id_bmf_numeraca.notnull()].copy()
aux['id_bmf_numeraca'] = aux['id_bmf_numeraca'].astype(int)
del titprivado_perc['id_bmf_numeraca']
aux = aux.drop_duplicates()
titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'], how='left')

# pagto_amortizacao
aux = titprivado_perc[['id_papel', 'indexador_dc_du']][titprivado_perc.indexador_dc_du.notnull()].copy()
aux['indexador_dc_du'] = aux['indexador_dc_du'].astype(int)
del titprivado_perc['indexador_dc_du']
aux = aux.drop_duplicates()
titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

# juros_cada
aux = titprivado_perc[['id_papel', 'juros_cada']][titprivado_perc.juros_cada.notnull()].copy()
aux['juros_cada'] = aux['juros_cada'].astype(int)
del titprivado_perc['juros_cada']
aux = aux.drop_duplicates()
titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'], how='left')

# indexador_dc_du
aux = titprivado_perc[['id_papel', 'indexador_dc_du']][titprivado_perc.indexador_dc_du.notnull()].copy()
aux['indexador_dc_du'] = aux['indexador_dc_du'].astype(int)
del titprivado_perc['indexador_dc_du']
aux = aux.drop_duplicates()
titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

# juros_dc_du
aux = titprivado_perc[['id_papel', 'juros_dc_du']][titprivado_perc.juros_dc_du.notnull()].copy()
aux['juros_dc_du'] = aux['juros_dc_du'].astype(int)
del titprivado_perc['juros_dc_du']
aux = aux.drop_duplicates()
titprivado_perc = titprivado_perc.merge(aux, on=['id_papel'])

# flag_inclusao
titprivado_perc['flag_inclusao'] = titprivado_perc['flag_inclusao'].astype(int)

# du_per
titprivado_perc['du_per'] = titprivado_perc['du_per'].astype(int)

# dc_per
titprivado_perc['dc_per'] = titprivado_perc['dc_per'].astype(int)

# dt_ref -> data_fim
titprivado_perc['dt_ref'] = pd.to_datetime(titprivado_perc['dt_ref'])
titprivado_perc['data_fim'] = np.where(titprivado_perc['indexador'] == 'DI1',
                                       titprivado_perc['dt_ref'] - pd.DateOffset(months=0, days=1),
                                       titprivado_perc['dt_ref'])

titprivado_perc['id_papel'] = titprivado_perc['id_papel_old']

titprivado_perc['data_mtm'] = titprivado_perc['data_referencia']
titprivado_perc['data_negociacao'] = titprivado_perc['data_referencia']

# Tabelas não necessárias - MTM
del titprivado_perc['data_referencia']
del titprivado_perc['id_papel_old']
del titprivado_perc['indice_du_mtm']
del titprivado_perc['indice_dc_mtm']
del titprivado_perc['ano_dt_ref2']
del titprivado_perc['mes_dt_ref2']
del titprivado_perc['dia_dt_ref2']
del titprivado_perc['vertices_positivo']
del titprivado_perc['indice_dc_dt_ref2']
del titprivado_perc['indice_du_dt_ref2']
del titprivado_perc['prazo_dc']
del titprivado_perc['ano_inicio']
del titprivado_perc['mes_inicio']
del titprivado_perc['dia_inicio']
del titprivado_perc['indice_du_inicio']
del titprivado_perc['indice_dc_inicio']
del titprivado_perc['ano_fim']
del titprivado_perc['mes_fim']
del titprivado_perc['dia_fim']
del titprivado_perc['indice_du_fim']
del titprivado_perc['indice_dc_fim']
del titprivado_perc['dt_ref']
del titprivado_perc['dtoperacao_mtm']
del titprivado_perc['dif']
del titprivado_perc['pu_mercado']
del titprivado_perc['pu_curva']
del titprivado_perc['mtm_mercado']
del titprivado_perc['mtm_curva']
del titprivado_perc['pu_regra_xml']
del titprivado_perc['mtm_regra_xml']
del titprivado_perc['data_spread']
del titprivado_perc['pu']
del titprivado_perc['dif_curva']
del titprivado_perc['pupar']
del titprivado_perc['indice_du_fim_spread']
del titprivado_perc['indice_du_inicio_spread']
del titprivado_perc['prazo_du_spread']
del titprivado_perc['mtm_spread']
del titprivado_perc['pv_spread']
del titprivado_perc['tx_spot_spread']
del titprivado_perc['tx_spot_ano_spread']
del titprivado_perc['fator_desconto_spread']

# Tabelas não necessárias - XML
del titprivado_perc['id_xml_debenture']
del titprivado_perc['isin']
del titprivado_perc['coddeb']
del titprivado_perc['dtemissao']
del titprivado_perc['dtoperacao']
del titprivado_perc['dtvencimento']
del titprivado_perc['cnpjemissor']
del titprivado_perc['qtdisponivel']
del titprivado_perc['qtgarantia']
del titprivado_perc['pucompra']
del titprivado_perc['puvencimento']
del titprivado_perc['puposicao']
del titprivado_perc['puemissao']
del titprivado_perc['principal']
del titprivado_perc['tributos']
del titprivado_perc['valorfindisp']
del titprivado_perc['valorfinemgar']
del titprivado_perc['coupom']
del titprivado_perc['percindex']
del titprivado_perc['caracteristica']
del titprivado_perc['percprovcred']
del titprivado_perc['classeoperacao']
del titprivado_perc['idinternoativo']
del titprivado_perc['nivelrsc']
del titprivado_perc['header_id']
del titprivado_perc['cusip']
del titprivado_perc['depgar']
del titprivado_perc['debconv']
del titprivado_perc['debpartlucro']
del titprivado_perc['SPE']
del titprivado_perc['dtretorno']
del titprivado_perc['puretorno']
del titprivado_perc['indexadorcomp']
del titprivado_perc['perindexcomp']
del titprivado_perc['txoperacao']
del titprivado_perc['classecomp']

# Remove as duplicatas de isin
titprivado_perc = titprivado_perc.sort(['codigo_isin', 'id_papel', 'data_fim'], ascending=[True, True, True])
titprivado_perc = titprivado_perc.drop_duplicates(subset=['codigo_isin', 'data_fim'])

# titprivado_perc['data_bd'] = horario_bd
titprivado_perc['data_bd'] = datetime.datetime.today()

titprivado_perc = titprivado_perc.where((pd.notnull(titprivado_perc)), None)

titprivado_perc['flag1'] = titprivado_perc['flag'].str[0:2]
del titprivado_perc['flag']
titprivado_perc = titprivado_perc.rename(columns={'flag1': 'flag'})

connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')
pd.io.sql.to_sql(titprivado_perc, name='mtm_renda_fixa', con=connection, if_exists='append', flavor='mysql', index=0)
connection.close()

###############################################################################
# 6 - Preenchimento tabela xml
###############################################################################

del original['id_xml_debenture']
del original['pu_mercado']
del original['mtm_mercado']
del original['pu_curva']
del original['mtm_curva']
del original['pu_regra_xml']
del original['mtm_regra_xml']
del original['data_referencia']

del titprivado['mtm']

titprivado = titprivado.merge(finalizacao, on=['codigo_isin'], how='left')

titprivado_xml = titprivado[titprivado.dt_ref == titprivado.data_referencia].copy()

titprivado_xml = titprivado_xml.drop_duplicates(subset=['id_papel'], take_last=True)

titprivado_xml = titprivado_xml.rename(columns={'mtm': 'mtm_calculado'})

anbima_deb = anbima_deb.rename(columns={'pu': 'pu_n'})

titprivado_xml = titprivado_xml.merge(anbima_deb, on=['coddeb', 'data_spread'], how='left')

titprivado_xml['pu'] = np.where(titprivado_xml['pu_n'].notnull(), titprivado_xml['pu_n'],
                                titprivado_xml['mtm_calculado'])

titprivado_xml['pu_mercado'] = np.where(titprivado_xml['caracteristica'] == 'N', titprivado_xml['pu'], 0)
titprivado_xml['pu_curva'] = np.where(titprivado_xml['caracteristica'] == 'V', titprivado_xml['pupar'], 0)

titprivado_xml = titprivado_xml[['id_papel', 'pu_mercado', 'pu_curva', 'data_referencia']].copy()

final = original.merge(titprivado_xml, on=['id_papel'], how='left')

final['data_referencia'] = dt_base_rel

final['pu_mercado'] = np.where((final['pu_mercado'].isnull()) | (final['pu_mercado'] == 0), final['puposicao'],
                               final['pu_mercado'])
final['pu_mercado'] = np.where(final['dtretorno'].notnull(), final['puposicao'], final['pu_mercado'])
final['mtm_mercado'] = final['pu_mercado'] * (final['qtdisponivel'] + final['qtgarantia'])

final['pu_curva'] = np.where(final['pu_curva'].isnull(), final['puposicao'], final['pu_curva'])
final['pu_curva'] = np.where(final['dtretorno'].notnull(), final['puposicao'], final['pu_curva'])
final['mtm_curva'] = final['pu_curva'] * (final['qtdisponivel'] + final['qtgarantia'])

final['pu_regra_xml'] = np.where(final['caracteristica'] == 'N', final['pu_mercado'], final['pu_curva'])
final['mtm_regra_xml'] = np.where(final['caracteristica'] == 'N', final['mtm_mercado'], final['mtm_curva'])

final['data_bd'] = datetime.datetime.today()

del final['dtrel']

final['indexador'] = final['indexador'].str.replace('IGPM', 'IGP')
final['indexador'] = final['indexador'].str.replace('IAP', 'IPC')
final['indexador'] = final['indexador'].str.replace('SEM-ÍNDICE', 'PRE')
final['indexador'] = final['indexador'].str.replace('ANB', 'DI1')
final['indexador'] = final['indexador'].str.replace('ANBID', 'DI1')
final['indexador'] = final['indexador'].str.replace('CDI', 'DI1')
final['indexador'] = final['indexador'].str.replace('IPCA', 'IPC')

connection = db.connect('localhost', user='root', passwd=senhabd, db='projeto_inv')
pd.io.sql.to_sql(final, name='xml_debenture', con=connection, if_exists='append', flavor='mysql', index=0)
connection.close()

horario_fim = datetime.datetime.now()
tempo = horario_fim - horario_inicio
print(tempo)

