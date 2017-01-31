def value_at_risk(vetor_exposicoes, matriz_cov, fc,t):

    import numpy as np
    import logging

    logger = logging.getLogger(__name__)

    mmult = np.dot(vetor_exposicoes.T, np.dot(matriz_cov, vetor_exposicoes))
    var = np.sqrt(mmult) * fc * t
    dollar_var = np.dot(matriz_cov, vetor_exposicoes)
    var_beta = dollar_var / mmult
    marginal_var = var_beta * var
    component_var = marginal_var * vetor_exposicoes

    logger.info("Retornando valors da função value_at_risk")
    return var, marginal_var, component_var
