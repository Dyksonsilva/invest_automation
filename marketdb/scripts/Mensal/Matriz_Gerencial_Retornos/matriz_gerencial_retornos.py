import datetime
import numpy as np
import pandas as pd
import pymysql as db
from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

id_relatorio_quaid419 = 3528

# Diretório de save de planilhas
save_path = full_path_from_database("matriz_gerencial_retornos")

# Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
dtbase = get_data_ultimo_dia_util_mes_anterior()
dt_base = dtbase[0] + '-' + dtbase[1] + '-' + dtbase[2]
dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]

data_inicial = "2010-03-31"
data_final = dt_base
lambda_geral = 0.95 #definir lambdas específicos na linha 60

# Ajuste datas
data_inicial = datetime.datetime.strptime(data_inicial, "%Y-%m-%d").date()
data_final = datetime.datetime.strptime(data_final, "%Y-%m-%d").date()

# Consulta valores das séries
connection = db.connect('localhost', user = 'root', passwd = "root", db = 'projeto_inv')

# Substituir dados do excel pelas informações do MySQL
dados_curvas = pd.read_sql_query('SELECT * FROM curva_ettj_vertices_fixos', connection)
dados_bacen = pd.read_sql_query('SELECT * FROM bacen_series', connection)
dados_cotas1 = pd.read_sql_query('SELECT * FROM projeto_inv.valoreconomico_cotas', connection)
dados_cotas1 = dados_cotas1.sort(['cota', 'dt_ref', 'data_bd'], ascending=[True, True, True])
dados_cotas1 = dados_cotas1.drop_duplicates(subset=['cota', 'dt_ref'], take_last=True)

data_bd = dados_cotas1['data_bd'].iloc[0]

dados_bacen = dados_bacen.sort(['data_referencia', 'codigo', 'data_bd'], ascending=[True, True, True])
dados_bacen = dados_bacen.drop_duplicates(subset=['codigo', 'data_referencia'], take_last=True)

dados_cotas = pd.read_excel(full_path_from_database('excels') + 'BRSND2CTF000.xlsx')

dados_cotas['dt_ref'] = dados_cotas['dt_ref'].astype(str)
dados_cotas['dt_ref'] = dados_cotas['dt_ref'].str.split('/')
dados_cotas['ano'] = dados_cotas['dt_ref'].str[2]
dados_cotas['mes'] = dados_cotas['dt_ref'].str[1]
dados_cotas['dia'] = dados_cotas['dt_ref'].str[0]
dados_cotas['dt_ref'] = pd.to_datetime('20'+dados_cotas['ano']+dados_cotas['mes']+dados_cotas['dia']).dt.date
del dados_cotas['ano']
del dados_cotas['mes']
#del dados_cotas['dia']
dados_cotas['data_bd'] = None
dados_cotas['data_bd'] = dados_cotas['data_bd'].fillna(data_bd)

dados_cotas = dados_cotas.append(dados_cotas1)
dados_cotas = dados_cotas.drop_duplicates(subset=['isin_fundo','dt_ref'])

## Filtrar datas para estimativa
dados_curvas = dados_curvas[(dados_curvas["dt_ref"]<data_final) & (dados_curvas["dt_ref"]>data_inicial)]
dados_bacen = dados_bacen[(dados_bacen["data_referencia"]<data_final) & (dados_bacen["data_referencia"]>data_inicial)]
dados_cotas = dados_cotas[(dados_cotas["dt_ref"]<data_final) & (dados_cotas["dt_ref"]>data_inicial)]

#Ajusta o nome de duas colunas
dados_bacen['nome'] = np.where(dados_bacen['codigo']==7,'Bovespa - índice',dados_bacen['nome'])
dados_bacen['nome'] = np.where(dados_bacen['codigo']==1,'Dólar comercial (venda)',dados_bacen['nome'])

#Tira os índices mensais
dados_bacen = dados_bacen[dados_bacen['frequencia']!="M"].copy()

#Tira as cotas cujo histórico é ruim
lista_fundos_excluidos = ['BRBVEPCTF002','BRFCLGCTF007','BRFCLGCTF015','BROLGSCTF005','BRSNGSCTF002', 'BRMBVACTF007','BRINSBCTF020', 'BRVCCDCTF000']

dados_cotas_chamber = dados_cotas[dados_cotas['isin_fundo'].isin(lista_fundos_excluidos)].copy()

dados_cotas_chamber['codigo'] = 7
dados_cotas_chamber = dados_cotas_chamber[['isin_fundo','codigo']].copy()
dados_cotas_chamber = dados_cotas_chamber.drop_duplicates()

chamber_corr = dados_bacen[dados_bacen['codigo']==7].copy()
dados_cotas_chamber = pd.merge(chamber_corr, dados_cotas_chamber, right_on=['codigo'], left_on=['codigo'], how='left')

dados_cotas_chamber['nome'] = dados_cotas_chamber['isin_fundo']

for i in lista_fundos_excluidos:
    dados_cotas_chamber['codigo'] = np.where(dados_cotas_chamber['nome'].isin([i]),np.random.rand(),dados_cotas_chamber['codigo'])

dados_cotas_chamber['codigo'] = dados_cotas_chamber['codigo']*100000
dados_cotas_chamber['codigo'] = dados_cotas_chamber['codigo'].astype(int)

del dados_cotas_chamber['isin_fundo']

dados_bacen = dados_bacen.append(dados_cotas_chamber)

dados_cotas = dados_cotas[~dados_cotas['isin_fundo'].isin(lista_fundos_excluidos)].copy()

####################################################################################
#
#     Leitura e tratamento das variancias da bmfbovespa - seleciona apenas as ações da carteira
#
####################################################################################

dados_vol_bmf = pd.read_excel(full_path_from_database("excels_bmf") + 'volatilidade_bmf_' + dtbase_concat + '.xlsx')
dados_vol_bmf.columns = ['codigo_negociacao', 'nome_empresa', 'preco_fechamento', 'ewma_anual']

acoes_ibov = pd.read_excel(full_path_from_database("excels_bmf") + 'Composição_ibovespa_' + dtbase_concat + '.xlsx')
acoes_ibov.columns = ['codigo', '1', '2', '3', '4']
acoes_ibov['codigo'] = acoes_ibov['codigo']

quaid_419 = pd.read_sql_query('SELECT * from projeto_inv.quaid_419 where id_relatorio_quaid419='+str(id_relatorio_quaid419) +' and FTRCODIGO = "AA1"', connection)
quaid_419 = quaid_419[quaid_419['data_bd']==max(quaid_419['data_bd'])]
quaid_419 = quaid_419.rename(columns={'EMFCODCUSTODIA':'codigo','EMFCODISIN':'2'})
quaid_419 = quaid_419[['codigo','2']].copy()

acoes_ibov = acoes_ibov.append(quaid_419)

lista_acoes = acoes_ibov['codigo'].unique()

dados_vol_bmf = dados_vol_bmf[dados_vol_bmf.codigo_negociacao.isin(lista_acoes)].copy()

dados_vol_bmf['codigo'] = 7

del dados_vol_bmf['nome_empresa']
del dados_vol_bmf['preco_fechamento']
del dados_vol_bmf['ewma_anual']

chamber_corr = dados_bacen[dados_bacen['codigo']==7].copy()

dados_vol_bmf = pd.merge(chamber_corr,dados_vol_bmf,right_on=['codigo'],left_on=['codigo'],how='left')
dados_vol_bmf['nome'] = dados_vol_bmf['codigo_negociacao'] + '_1'

for i in lista_acoes:
    dados_vol_bmf['codigo'] = np.where(dados_vol_bmf['nome'].isin([i+'_1']),np.random.rand(),dados_vol_bmf['codigo'])

dados_vol_bmf['codigo'] = dados_vol_bmf['codigo']*100000
dados_vol_bmf['codigo'] = dados_vol_bmf['codigo'].astype(int)

del dados_vol_bmf['codigo_negociacao']

dados_bacen = dados_bacen.append(dados_vol_bmf)

# dados_bacen.to_excel(save_path+'dados_bacen.xlsx')

## Lista de séries do Bacen a ser utlizada
aux1 = dados_vol_bmf[['codigo','nome']].copy()
aux1 = aux1.drop_duplicates()
aux2 = dados_cotas_chamber[['codigo','nome']].copy()
aux2 = aux2.drop_duplicates()
aux2 = aux2.append(aux1)
lista_series_bacen = aux2['codigo'].tolist()


## Lista_series_bacen = lista_series_bacen.tolist()
n_len = len(lista_series_bacen)
lista_series_bacen.insert(n_len,1)
n_len = len(lista_series_bacen)
lista_series_bacen.insert(n_len,7)

## Ajuste para variáveis virarem colunas
tabela_ajustada= pd.pivot_table(dados_curvas,index=["dt_ref"], columns=["indexador_cod","prazo"], values =["tx_spot_ano"])

## Ajuste data Cupom Cambial (de 252 para 360)
nova_lista = []
for i in range(len(tabela_ajustada.columns)):
    if tabela_ajustada.columns[i][1]=="DP":
        resultado = (tabela_ajustada.columns[i][0],"DP",int(tabela_ajustada.columns[i][2]*(360/252.)))
        
    else:
        resultado=tabela_ajustada.columns[i]
    nova_lista.append(resultado)
    
tabela_ajustada.columns = nova_lista
    
## Ajuste data para PU
tabela_pu = tabela_ajustada
for coluna in tabela_ajustada.columns:
    periodo = coluna[2]
    taxa = coluna[1]
    if taxa == "DP":
        fator_data = (periodo/360.)
    else:
        fator_data = (periodo/252.)
    tabela_pu[coluna] = 100/((1+tabela_ajustada[coluna])**fator_data)

######################################
## Adicionar valores vindos do Bacen
######################################

#Guarda valores para a proxy das cotas
proxy_cotas = dados_bacen[['data_referencia','codigo','valor']][dados_bacen["codigo"]==7].copy()

tabela_pu_aux = tabela_pu
for serie in lista_series_bacen:
    try:
        dados_serie = dados_bacen[dados_bacen["codigo"] == serie]
        dados_serie = dados_serie.drop_duplicates(subset=['data_referencia','codigo'])
        tabela_pu_aux = pd.merge(tabela_pu_aux, dados_serie[["valor","data_referencia"]], left_index = True, right_on =["data_referencia"], how ="left")
        tabela_pu_aux.rename(columns = {'valor':dados_serie["nome"].iloc[0]}, inplace=True)
        tabela_pu_aux.index = tabela_pu_aux["data_referencia"]
        del tabela_pu_aux["data_referencia"]
    except:
        pass

##############################
## Adicionar séries de cotas
##############################

# Ajustar a proxy das cotas - Ibovespa - serie historica que esta no bacen
dados_cotas_proxy = pd.DataFrame(columns=['codigo','isin_fundo'])
dados_cotas_proxy1 = []

lista_isin_fundo = dados_cotas['isin_fundo'].unique()
for i in lista_isin_fundo:
    dados_cotas_proxy1.append(i)
    
dados_cotas_proxy['isin_fundo'] = dados_cotas_proxy1
dados_cotas_proxy['codigo'] = 7

#Dropa duplicatas bacen
proxy_cotas = proxy_cotas.drop_duplicates(subset=['data_referencia','codigo'])
proxy_cotas.columns = ['dt_ref', 'codigo', 'cota']

dados_cotas_proxy = pd.merge(dados_cotas_proxy, proxy_cotas, left_on=['codigo'], right_on=['codigo'], how='left')
dados_cotas = dados_cotas_proxy[['dt_ref', 'isin_fundo', 'cota']].copy()

###############################################################################

tabela_cotas = pd.pivot_table(dados_cotas,index=["dt_ref"], columns=["isin_fundo"], values =["cota"])

tabela_pu_aux = tabela_pu_aux.merge(tabela_cotas,left_index=True, right_index=True)

## Preencher datas vazias com o último valor disponível
tabela_pu_aux.fillna(method='pad', inplace=True)
    
## Preencher o histórico de nan com o primeiro valor da série
tabela_pu_aux.fillna(method='bfill', inplace=True)

############################################################################
## Cálculo do retorno diário
############################################################################

retornos = tabela_pu_aux.pct_change()

## Retirar primeira observação NaN
retornos = retornos[1:]

aux = retornos**2
aux.to_excel(save_path + 'retornos' + str(data_final) + '.xlsx')

###############################################################################
# EXECUTAR PRIMEIRAMENTE ATE ESSE PONTO, E ENTAO, ATUALIZAR OS ARQUIVOS EXCEL

## Cálculo da matriz de correlação
matriz_correlacao = retornos.corr()
matriz_correlacao.fillna(0, inplace=True)

# Julho
variancias_manual = pd.read_excel(root + '/codigo_fonte_final/HDI/Cotas Valor/variancias_' + dtbase + '.xlsx')
# Junho
# variancias_manual = pd.read_excel('C:/Users/Cora.Santos/Desktop/HDI-Investimentos/codigo_fonte_final/HDI/Cotas valor/variancias_junho.xlsx')
# Maio
# variancias_manual = pd.read_excel('C:/Users/Cora.Santos/Desktop/HDI-Investimentos/codigo_fonte_final/HDI/Cotas valor/variancias_maio.xlsx')

# variancias = variancias_manual[2:]

'''COMO QUE EU COLOCO NO FORMATO QUE ESTAVAM VINDO AS VARIANCIAS ANTES?'''
variancias = variancias_manual.values
desvio_padrao = np.sqrt(variancias)
desvio_padrao = np.squeeze(desvio_padrao)

## Cálculo da matriz de covariância
matriz_diagonal = np.diag(desvio_padrao)
matriz_covariancias = np.dot(matriz_diagonal, np.dot(matriz_correlacao, matriz_diagonal))

retornos.columns[0][2]

##############################################################################
## Salvar no banco de dados
base_final = pd.DataFrame()


## Padronizar nome dos vértices
def definicao_nome(coluna):
    if ("TP" in coluna) & ("cota" not in coluna):
        return "TR"
    elif ("DP" in coluna) & ("cota" not in coluna):
        return "CCAMBIAL"
    elif ("DIM" in coluna) & ("cota" not in coluna):
        return "IGPM"
    elif ("DIC" in coluna) & ("cota" not in coluna):
        return "IPCA"
    elif ("PRE" in coluna) & ("cota" not in coluna):
        return "PRE"
    elif ("TR" in coluna) & ("cota" not in coluna):
        return "TR"
    elif ("IGP-M" in coluna) & ("cota" not in coluna):
        return "IGPM"
    elif ("IPCA" in coluna) & ("cota" not in coluna):
        return "IPCA"
    elif ("Dólar" in coluna) & ("cota" not in coluna):
        return "DOL"
    elif ("Bovespa" in coluna) & ("cota" not in coluna):
        return "IBOV"
    elif ("IC" in coluna) & ("cota" not in coluna):
        return "COMMODITIES"
    else:
        if isinstance(coluna, tuple):
            return coluna[1]
        else:
            nome = coluna.split('_')
            nome = nome[0]
            return nome


for linha in range(len(retornos.columns)):
    for coluna in range(linha + 1):
        resultado = {}
        if isinstance(retornos.columns[linha], tuple) and len(retornos.columns[linha]) > 2:
            resultado["linha"] = definicao_nome(retornos.columns[linha][1]) + "_" + str(retornos.columns[linha][2])
        else:
            resultado["linha"] = definicao_nome(retornos.columns[linha])
        if isinstance(retornos.columns[coluna], tuple) and len(retornos.columns[coluna]) > 2:
            resultado["coluna"] = definicao_nome(retornos.columns[coluna][1]) + "_" + str(retornos.columns[coluna][2])
        else:
            resultado["coluna"] = definicao_nome(retornos.columns[coluna])
        resultado["valor"] = matriz_covariancias[linha][coluna]
        base_final = base_final.append(resultado, ignore_index=True)

max(base_final["valor"])
base_final["data_bd"] = datetime.datetime.now()
base_final["data_inicial"] = data_inicial
base_final["data_final"] = data_final
base_final["horizonte"] = "dia"
base_final["lambda"] = lambda_geral

################################################################################
#
# TESTE SE VETOR VEM ZERADO
#
################################################################################


## Inserir "matriz_id"

matriz_id = pd.read_sql_query('SELECT max(matriz_id) as matriz_id FROM matriz_gerencial', connection)
if matriz_id['matriz_id'][0] == None:
    matriz_id_valor = 0
else:
    matriz_id_valor = matriz_id.get_value(0, 'matriz_id').astype(int) + 1

#
# matriz_id = matriz_id["max(matriz_id)"][0]
# try:
#    matriz_id+=1
# except:
#    matriz_id=1

base_final["matriz_id"] = matriz_id_valor

### Salvar no BD
pd.io.sql.to_sql(base_final, name='matriz_gerencial', con=connection, if_exists="append", flavor='mysql', index=0,
                 chunksize=5000)

# matriz_gerencial = pd.read_sql_query('SELECT * FROM projeto_inv.matriz_gerencial where matriz_id = "1"', connection)
base_final.to_excel(save_path + 'matriz_gerencial_' + str(data_final) + '.xlsx')
retornos.to_excel(save_path + 'retornos_mensal_' + str(data_final) + '.xlsx')
#################################################################################
#
### Reconstrução da Matriz
# matriz_reconstruida = pd.DataFrame(columns= base_final["linha"].unique())
#
# matriz_reconstruida[matriz_reconstruida.columns[0]]= matriz_reconstruida.columns
#
# for coluna in matriz_reconstruida.columns:
#    for i in range(len(matriz_reconstruida.columns)):
#        linha = matriz_reconstruida.columns[i]
#        try:
#            matriz_reconstruida[coluna].iloc[i]=base_final[(base_final["coluna"]==coluna) & (base_final["linha"]==matriz_reconstruida.columns[i])]["valor"].values[0]
#        except:
#            matriz_reconstruida[coluna].iloc[i]= base_final[(base_final["linha"]==coluna) & (base_final["coluna"]==matriz_reconstruida.columns[i])]["valor"].values[0]

horario_fim = datetime.datetime.now()
tempo = horario_fim - horario_inicio
print(tempo)