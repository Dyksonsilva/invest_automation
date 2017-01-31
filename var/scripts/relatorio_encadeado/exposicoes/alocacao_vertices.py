#### Método para alocação nos vértices

def alocacao_vertices(valor, du_observado, du_anterior, du_posterior):

    import logging
    logger = logging.getLogger(__name__)

    ## Ajuste para du_observado anterior ao primeiro vértice
    if du_anterior == 1:
        du_anterior = 0
    alocacao_anterior = valor * (du_posterior - du_observado) / (du_posterior - du_anterior) if du_anterior > 0 else 0
    alocacao_posterior = valor * (du_observado - du_anterior) / (du_posterior - du_anterior) if du_posterior > 0 else 0

    ## Ajuste para quando o du_observado ser um vértice padrão
    if du_observado == du_anterior:
        alocacao_anterior = valor
        alocacao_posterior = 0

    logger.info("Retornando valors da função alocacao_vertices")
    return alocacao_anterior, alocacao_posterior
