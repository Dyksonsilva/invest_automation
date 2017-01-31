def definicao_lambda (coluna):

    lambda_geral = 0.95

    # Definição Lambdas
    lista_lambdas = {'PRE': 92, 'DIC': 94, 'DIM': 85, 'DP': 73, 'TP': 97, "TR": 97, "IPCA": 94, "IGPM": 86, "Dólar": 78,
                     "Bovespa": 95, "ICB": 95}

    if "TP" in coluna:
        return lista_lambdas["TP"]
    elif "DP" in coluna:
        return lista_lambdas["DP"]
    elif "DIM" in coluna:
        return lista_lambdas["DIM"]
    elif "DIC" in coluna:
        return lista_lambdas["DIC"]
    elif "PRE" in coluna:
        return lista_lambdas["PRE"]
    elif "TR" in coluna:
        return lista_lambdas["TR"]
    elif "IGP-M" in coluna:
        return lista_lambdas["IGPM"]
    elif "IPCA" in coluna:
        return lista_lambdas["IPCA"]
    elif "Dólar" in coluna:
        return lista_lambdas["Dólar"]
    elif "Bovespa" in coluna:
        return lista_lambdas["Bovespa"]
    elif "IC" in coluna:
        return lista_lambdas["ICB"]
    else:
        return lambda_geral