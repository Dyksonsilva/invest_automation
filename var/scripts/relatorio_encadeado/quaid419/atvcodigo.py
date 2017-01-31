## Código para encontrar o ATVCODIGO dada a descrição do produto
def atvcodigo(linha):
    if linha == "debênture":
        return "A1001"
    elif linha == "caixa":
        return "A0001"
    elif linha == "título público":
        return "A1001"
    elif "ompromissada" in linha:
        return "A1005"
    elif "blico" in linha:
        return "A1001"
    elif linha == "título privado":
        return "A1001"
    elif linha == "ações":
        return "A1002"
    elif linha == "valores a receber":
        return "A0001"
    elif linha == "swap":
        return "D0003"
    elif linha == "Futuro":
        return "D0001"
    elif "termo" in linha:
        return "D0002"
    elif "Termo" in linha:
        return "D0002"
    elif "juste" in linha:
        return "D0001"
    elif linha == "fundo":
        return "A1003"
    elif "espesas" in linha:
        return "A0001"
    elif "Provisão" in linha:
        return "A0001"
    elif "tributos" in linha:
        return "A0001"
    elif "Corretagem" in linha:
        return "A0001"
    elif "resgatar" in linha:
        return "A0001"
    elif "emitir" in linha:
        return "A0001"
    elif "pagar" in linha:
        return "A0001"
    elif "receber" in linha:
        return "A0001"
    elif "futuro" in linha:
        return "D0001"