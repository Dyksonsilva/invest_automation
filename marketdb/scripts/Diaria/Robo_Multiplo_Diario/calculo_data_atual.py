def calculo_data_atual():

	import pandas as pd
	import pymysql as db
	import datetime
	import bizdays
	import logging

	from invest_automation.settings import BASE_DIR

	# Get an instance of a logger
	logger = logging.getLogger(__name__)

	BASE_DIR = BASE_DIR.replace('\\','/')
	#### parametrizar estrutura com variaveis Globais
	connection=db.connect('localhost', user = 'root', passwd = 'root', db = 'projeto_inv')
	logging.info("Conexão com DB executada com sucesso")

	path_feriados_nacionais = pd.read_sql_query('SELECT path from projeto_inv.paths where filename="feriados_nacionais"', connection)
	#Fecha conexão com DB
	connection.close()

	logging.info("Leitura do DB executada com sucesso")
	path_feriados_nacionais = BASE_DIR + str(path_feriados_nacionais["path"].iloc[0])

	hoje = datetime.datetime.now()

	logging.info("Definindo calendário Anbima")
	# Calendário Anbima
	calendario_anbima=pd.read_excel(path_feriados_nacionais)
	feriados=calendario_anbima['Data'][0:936]
	cal=bizdays.Calendar(holidays=feriados, startdate='2000-01-01', weekdays=['sunday', 'saturday'])

	logging.info("Calculando último dia útil")
	# Pegar último dia útil
	delta_dias=datetime.timedelta(-1)
	dia_busca=hoje+delta_dias
	fator_diautil=0

	logging.info("Ajustando se o dia anterior não for dia útil")
	#Ajuste se o dia anterior não for dia útil
	while not cal.isbizday(dia_busca):
		fator_diautil=1
		dia_busca=dia_busca+delta_dias

	dia=str(dia_busca.day)
	mes=str(dia_busca.month)
	ano=str(dia_busca.year)

	logging.info("Calculo data base executado com sucesso")

	if len(mes)==1:
		mes="0"+mes

	if len(dia)==1:
		dia="0"+dia

	return ano, mes, dia, fator_diautil