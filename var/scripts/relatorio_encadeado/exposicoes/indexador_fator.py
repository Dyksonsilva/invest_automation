def indexador_fator(fator, relatorio, isin_fundo):
    if fator == "JJ1":
        return "PRE"
    elif "JM" in fator:
        return "CCAMBIAL"
    elif "JT" in fator:
        return "TR"
    elif fator == "JI1":
        return "IPCA"
    elif fator == "JI2":
        return "IGPM"
    elif fator == "JI8":
        return "IPCA"
    elif fator == "JI9":
        return "IGPM"
    elif "ME" in fator:
        return "DOL"
    elif "AA" in fator:
        return "IBOV"
    elif "MC" in fator:
        return "COMMODITIES"
    elif "FF1" in fator and relatorio == "R":
        return "IBOV"
    elif "FF1" in fator and relatorio == "G":
        return isin_fundo
    elif "998" in fator:
        return "PRE"
    elif "TS1" in fator:
        return "PERC"
    elif "TD1" in fator:
        return "PERC"
    else:
        return ""