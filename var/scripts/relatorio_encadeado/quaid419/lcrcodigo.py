### Código para dar o LCRCODIGO
def lcrcodigo(linha):
    if "blico" in linha:
        return "N01"
    elif "privado" in linha:
        return "N02"
    elif "uturo" in linha:
        return "N03"
    elif "swap" in linha:
        return "N03"
    elif "ermo" in linha:
        return "N03"
    elif "ações":
        return "N04"
    else:
        return "N05"