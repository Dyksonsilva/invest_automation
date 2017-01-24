def dic_tabela(caminho, categoria, tabela, header_id):

    import pandas as pd

    try:
        for i in range(len(caminho[categoria])):
            titulo = {}
            for coluna in caminho[categoria][i]:
                if isinstance(caminho[categoria][i][coluna],
                              dict):  # Caso exista uma subseção (por exemplo: Compromisso, Aluguel)
                    for subcoluna in caminho[categoria][i][coluna]:
                        titulo[subcoluna] = caminho[categoria][i][coluna][subcoluna]
                        if subcoluna not in tabela:
                            tabela[subcoluna] = ""

                else:
                    titulo[coluna] = caminho[categoria][i][coluna]
                    if coluna not in tabela:
                        tabela[coluna] = ""
            if 'header_id' not in tabela:  # adicionar coluna "header_id" caso ela não exista
                tabela['header_id'] = ""
            titulo['header_id'] = header_id
            tabela = tabela.append(pd.Series(titulo), ignore_index=True)

    except:  # caso com apenas uma linha (ex: header)
        titulo = {}
        for coluna in caminho[categoria]:
            if isinstance(caminho[categoria][coluna], dict):
                for subcoluna in caminho[categoria][coluna]:
                    titulo[subcoluna] = caminho[categoria][coluna][subcoluna]
                    if subcoluna not in tabela:
                        tabela[subcoluna] = ""

            else:
                titulo[coluna] = caminho[categoria][coluna]
                if coluna not in tabela:
                    tabela[coluna] = ""
        if 'header_id' not in tabela:  # adicionar coluna "header_id" caso ela não exista
            tabela['header_id'] = ""
        titulo['header_id'] = header_id
        tabela = tabela.append(pd.Series(titulo), ignore_index=True)
    return tabela
