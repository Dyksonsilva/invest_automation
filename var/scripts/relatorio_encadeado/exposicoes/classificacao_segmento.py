def classificacao_segmento(codigo):
    if codigo == "A1001":
        return "RF"
    elif codigo == "A1002":
        return "RV"
    elif codigo in ("A0001", "A1005"):
        return "Caixa"
    elif "D" in codigo:
        return "DERIVATIVOS"
    else:
        return "Cotas"