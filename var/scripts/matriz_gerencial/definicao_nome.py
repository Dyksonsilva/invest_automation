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