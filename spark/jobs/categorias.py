"""
Classificacao por CATEGORIA: a dimensao qualitativa do sistema, complementar ao score.

Python puro, sem dependencias — roda nos testes sem Docker, sem Spark e sem rede.

Decisao de desenho: todas as faixas aqui sao BANDAS ABSOLUTAS, nunca quartis do universo.
Com ~14 ativos, um corte por quartil garantiria ~3 ativos em cada faixa independentemente
de o universo ser todo de empresas gigantes ou todo de micro caps — o rotulo deixaria de
significar algo no mundo real e passaria a significar apenas "posicao relativa nesta
amostra". Bandas fixas sao estaveis, comparaveis entre execucoes e auditaveis.
"""

import re

# --- Classe do ativo, pelo sufixo numerico do ticker na B3 ---------------------------
CLASSE_ACAO_ON = "ACAO_ON"
CLASSE_ACAO_PN = "ACAO_PN"
CLASSE_UNIT_OU_FII = "UNIT_OU_FII"
CLASSE_BDR = "BDR"
CLASSE_OUTRO = "OUTRO"

# --- Porte, por valor de mercado em BRL ----------------------------------------------
LIMITE_MICRO = 2_000_000_000
LIMITE_SMALL = 10_000_000_000
LIMITE_MID = 50_000_000_000

# --- Perfil de risco, por volatilidade anualizada ------------------------------------
LIMITE_VOL_BAIXA = 0.25
LIMITE_VOL_MEDIA = 0.40

# --- Perfil de investidor -------------------------------------------------------------
DY_MINIMO_DIVIDENDOS = 0.06
SCORE_MINIMO_VALOR_VALUATION = 70.0
SCORE_MINIMO_VALOR_SOLIDEZ = 60.0
CRESCIMENTO_MINIMO_RECEITA = 0.15

# Palavras que identificam setor/industria do ramo financeiro. Bancos e seguradoras nao
# possuem lucro bruto, EBITDA nem capital de giro no sentido industrial, entao varios
# indicadores do Fundamentus simplesmente nao se aplicam a eles — o que muda o perfil de
# pesos do score (ver pontuacao.PESOS_METRICA).
PALAVRAS_SETOR_FINANCEIRO = (
    "financ",
    "banco",
    "bancar",
    "seguro",
    "segurad",
    "previdenc",
    "credito",
    "capitaliza",
)


def classe_ativo_do_ticker(ticker: str) -> str:
    """Deduz a classe do ativo pelo sufixo numerico, convencao da B3.

    3 = ordinaria, 4/5/6 = preferenciais, 11 = unit ou FII, 3x = BDR.
    """
    if not ticker:
        return CLASSE_OUTRO

    casamento = re.search(r"(\d+)$", ticker.strip().upper())
    if not casamento:
        return CLASSE_OUTRO

    sufixo = casamento.group(1)
    if sufixo == "3":
        return CLASSE_ACAO_ON
    if sufixo in ("4", "5", "6", "7", "8"):
        return CLASSE_ACAO_PN
    if sufixo == "11":
        return CLASSE_UNIT_OU_FII
    # BDRs usam dois digitos na faixa 31-39 (ex: MUTC34, AAPL34).
    if len(sufixo) == 2 and sufixo.startswith("3"):
        return CLASSE_BDR
    return CLASSE_OUTRO


def eh_setor_financeiro(setor: str | None, industria: str | None = None) -> bool:
    """Heuristica por palavra-chave. A brapi devolve setor/industria ja em portugues."""
    texto = " ".join(filter(None, (setor, industria))).lower()
    return any(palavra in texto for palavra in PALAVRAS_SETOR_FINANCEIRO)


def porte_por_valor_mercado(valor_mercado: float | None) -> str | None:
    if valor_mercado is None or valor_mercado <= 0:
        return None
    if valor_mercado < LIMITE_MICRO:
        return "MICRO"
    if valor_mercado < LIMITE_SMALL:
        return "SMALL"
    if valor_mercado < LIMITE_MID:
        return "MID"
    return "LARGE"


def perfil_risco(volatilidade_anualizada: float | None) -> str | None:
    if volatilidade_anualizada is None:
        return None
    if volatilidade_anualizada < LIMITE_VOL_BAIXA:
        return "BAIXO"
    if volatilidade_anualizada < LIMITE_VOL_MEDIA:
        return "MEDIO"
    return "ALTO"


def perfil_investidor(
    dividend_yield: float | None,
    risco: str | None,
    score_valuation: float | None,
    score_solidez: float | None,
    cresc_receita_12m: float | None,
) -> str:
    """Rotulo de a que tipo de tese o ativo melhor serve.

    A ordem das regras e intencional: dividendos primeiro (tese mais restritiva, exige DY
    alto E risco controlado), depois valor, depois crescimento. O rotulo final e um
    fallback honesto, nao uma categoria de qualidade.
    """
    if (
        dividend_yield is not None
        and dividend_yield >= DY_MINIMO_DIVIDENDOS
        and risco != "ALTO"
    ):
        return "DIVIDENDOS"

    if (
        score_valuation is not None
        and score_valuation >= SCORE_MINIMO_VALOR_VALUATION
        and score_solidez is not None
        and score_solidez >= SCORE_MINIMO_VALOR_SOLIDEZ
    ):
        return "VALOR"

    if (
        cresc_receita_12m is not None
        and cresc_receita_12m >= CRESCIMENTO_MINIMO_RECEITA
        and risco != "BAIXO"
    ):
        return "CRESCIMENTO"

    return "ESPECULATIVO"


def classificar(linha: dict) -> dict:
    """Aplica todas as categorias de uma vez sobre uma linha ja pontuada."""
    risco = perfil_risco(linha.get("volatilidade_anualizada"))

    # Ativo sem dados suficientes nao recebe rotulo de tese. Deixar o fallback
    # "ESPECULATIVO" aqui seria uma afirmacao sobre o ativo que os dados nao sustentam —
    # ausencia de informacao nao e a mesma coisa que perfil especulativo. Classe, porte e
    # risco continuam sendo preenchidos quando mensuraveis, porque sao fatos observaveis.
    perfil = None
    if linha.get("elegivel_score") is not False:
        perfil = perfil_investidor(
            linha.get("dividend_yield"),
            risco,
            linha.get("score_valuation"),
            linha.get("score_solidez"),
            linha.get("cresc_receita_12m"),
        )

    return {
        "classe_ativo": linha.get("classe_ativo")
        or classe_ativo_do_ticker(linha.get("ticker", "")),
        "porte": porte_por_valor_mercado(linha.get("valor_mercado")),
        "perfil_risco": risco,
        "perfil_investidor": perfil,
    }
