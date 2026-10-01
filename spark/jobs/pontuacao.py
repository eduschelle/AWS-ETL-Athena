"""
Motor de pontuacao: transforma indicadores fundamentalistas em um score 0-100 e nota A-E.

Python puro, sem pyspark e sem rede.

TRES DECISOES DE DESENHO QUE VALE ENTENDER ANTES DE MEXER:

1. BANDAS ABSOLUTAS COMO BASE, nao percentis. Um score construido sobre a posicao relativa
   do ativo dentro do universo nao diz se a empresa e boa — diz apenas se ela e melhor que
   as outras 13 da amostra. Pior: com n pequeno, um unico outlier redefine toda a escala.
   Aqui cada indicador e convertido por interpolacao linear entre dois pontos de referencia
   de dominio ("P/L 25 e ruim, P/L 4 e otimo"), o que torna o score estavel entre execucoes,
   comparavel no tempo e independente do tamanho da amostra.

2. O PERCENTIL ENTRA SO COMO MODIFICADOR DE 30%, e apenas quando o grupo de comparacao tem
   pelo menos MIN_GRUPO ativos. Comparar o P/L de um banco com o de uma mineradora nao faz
   sentido, mas com ~14 ativos vários setores tem 1 ou 2 integrantes — e percentil com n=1 e
   ruido determinístico. A cascata setor -> macro-grupo -> universo resolve isso, e o grupo
   efetivamente usado fica gravado em `grupo_usado`/`n_grupo`, visivel no dado.

3. NULL NUNCA VIRA ZERO. Indicador ausente e removido do calculo e os pesos restantes sao
   RENORMALIZADOS; se sobrar peso de menos, o resultado e NULL em vez de um numero
   enganosamente preciso. A cobertura efetiva e exposta em `cobertura_indicadores_pct` para
   que quem consulta possa filtrar — deliberadamente NAO aplicamos uma penalidade oculta
   (do tipo `score * cobertura ** 0.25`), que tornaria o numero nao auditavel.
"""

import math

# Pontos de referencia por indicador: (valor_ruim -> nota 0, valor_bom -> nota 100).
# Para indicadores em que "menor e melhor", `ruim` e maior que `bom` — a interpolacao lida
# com isso naturalmente porque o denominador fica negativo.
BANDAS = {
    "preco_lucro": (25.0, 4.0),
    "preco_valor_patrimonial": (4.0, 0.6),
    "psr": (5.0, 0.5),
    "ev_ebit": (20.0, 4.0),
    "preco_ativo": (2.0, 0.3),
    "roe": (0.00, 0.25),
    "roic": (0.00, 0.20),
    "margem_liquida": (0.00, 0.20),
    "margem_ebit": (0.00, 0.25),
    "divida_liquida_patrimonio": (2.0, 0.0),
    "divida_liquida_ebitda": (3.5, 0.0),
    "liq_corrente": (0.8, 2.5),
    "dividend_yield": (0.01, 0.08),
}

# Indicadores em que um valor <= 0 nao e "muito barato", e sinal de prejuizo ou patrimonio
# negativo: a razao perde significado economico e a nota vai direto a zero.
NAO_POSITIVO_E_PESSIMO = (
    "preco_lucro",
    "preco_valor_patrimonial",
    "ev_ebit",
    "psr",
    "preco_ativo",
)

# Indicadores de endividamento em que um valor negativo e o melhor cenario possivel
# (a empresa tem mais caixa do que divida).
NEGATIVO_E_OTIMO = ("divida_liquida_patrimonio", "divida_liquida_ebitda")

PESOS_PILAR = {
    "valuation": 0.30,
    "rentabilidade": 0.30,
    "solidez": 0.25,
    "dividendos": 0.15,
}

PESOS_METRICA = {
    "padrao": {
        "valuation": {
            "preco_lucro": 0.30,
            "preco_valor_patrimonial": 0.25,
            "ev_ebit": 0.25,
            "psr": 0.10,
            "preco_ativo": 0.10,
        },
        "rentabilidade": {
            "roe": 0.35,
            "roic": 0.35,
            "margem_liquida": 0.20,
            "margem_ebit": 0.10,
        },
        "solidez": {
            "divida_liquida_patrimonio": 0.40,
            "divida_liquida_ebitda": 0.30,
            "liq_corrente": 0.30,
        },
        "dividendos": {"dividend_yield": 1.00},
    },
    # Bancos e seguradoras nao tem capital de giro nem EBITDA no sentido industrial:
    # liquidez corrente e divida/EBITDA sao estruturalmente inaplicaveis, e margem EBIT
    # tambem nao se traduz. Manter esses pesos faria o pilar cair por "falta de dados"
    # quando na verdade a metrica nao existe para o setor.
    "financeiro": {
        "valuation": {
            "preco_lucro": 0.30,
            "preco_valor_patrimonial": 0.25,
            "ev_ebit": 0.25,
            "psr": 0.10,
            "preco_ativo": 0.10,
        },
        "rentabilidade": {"roe": 0.45, "roic": 0.25, "margem_liquida": 0.30},
        "solidez": {"divida_liquida_patrimonio": 1.00},
        "dividendos": {"dividend_yield": 1.00},
    },
}

# Minimo de ativos num grupo para que a comparacao relativa signifique algo.
MIN_GRUPO = 5
PESO_BANDA = 0.70
PESO_PERCENTIL = 0.30

# Minimo de peso disponivel para que o resultado seja publicado em vez de NULL.
COBERTURA_MINIMA_PILAR = 0.50
COBERTURA_MINIMA_SCORE = 0.60

# Cortes FIXOS, nunca quintis: um universo inteiro de ativos ruins deve produzir varios E,
# e nao redistribuir as notas para dar ~20% de A de qualquer jeito.
CORTES_NOTA = ((80.0, "A"), (65.0, "B"), (50.0, "C"), (35.0, "D"))
NOTA_SEM_DADOS = "SD"

CONFIANCA_ALTA = 0.80
CONFIANCA_MEDIA = 0.60

# DY acima disto normalmente indica dividendo extraordinario, erro de dado ou "dividend
# trap" — a nota e maxima, mas o ativo fica sinalizado.
DY_SUSPEITO = 0.15

# Taxa de desconto do metodo Bazin: preco-teto e o preco que entregaria 6% de yield.
YIELD_ALVO_BAZIN = 0.06
# Constante de Graham para a formula do valor intrinseco: sqrt(22.5 * LPA * VPA).
CONSTANTE_GRAHAM = 22.5

# --- Sinal tecnico -------------------------------------------------------------------
# Combina QUALIDADE (score composto) com PRECO (margem de seguranca de Graham). Um ativo
# otimo e caro nao e um sinal forte, e um ativo barato e ruim tambem nao.
#
# Vocabulario deliberadamente tecnico (FORTE/NEUTRO/FRACO) em vez de COMPRA/VENDA: a saida
# e a classificacao de um modelo sobre dados parcialmente sinteticos e com cotacao atrasada
# ~30min, nao recomendacao de investimento.
LIMITE_SCORE_FORTE = 65.0
LIMITE_SCORE_FRACO = 50.0
# Abaixo disto o ativo esta muito acima do valor intrinseco estimado.
LIMITE_MARGEM_CARO = -0.20

SINAL_FORTE = "FORTE"
SINAL_NEUTRO = "NEUTRO"
SINAL_FRACO = "FRACO"
SINAL_SEM_DADOS = "SEM_DADOS"


def nota_banda(valor, ruim: float, bom: float) -> float | None:
    """Interpola linearmente o valor entre os pontos de referencia, limitando a [0, 100]."""
    if valor is None:
        return None
    if ruim == bom:
        raise ValueError("Pontos de referencia da banda nao podem ser iguais")
    bruta = 100.0 * (valor - ruim) / (bom - ruim)
    return max(0.0, min(100.0, bruta))


def nota_indicador(nome: str, valor, linha: dict | None = None) -> float | None:
    """Nota 0-100 de um indicador isolado, aplicando as regras economicas especiais."""
    if valor is None or nome not in BANDAS:
        return None

    if nome in NAO_POSITIVO_E_PESSIMO and valor <= 0:
        return 0.0

    if nome in NEGATIVO_E_OTIMO and valor < 0:
        # Divida liquida negativa = caixa liquido. Mas se o indicador ficou negativo por
        # causa de um EBITDA negativo (denominador), o sinal engana: a nota deve ser zero.
        if nome == "divida_liquida_ebitda" and linha is not None:
            ebitda = linha.get("ebitda")
            if ebitda is not None and ebitda <= 0:
                return 0.0
        return 100.0

    if nome == "dividend_yield" and valor > DY_SUSPEITO:
        return 100.0

    ruim, bom = BANDAS[nome]
    return nota_banda(valor, ruim, bom)


def percentil_midrank(valor, valores, melhor_maior: bool) -> float | None:
    """Posicao relativa em [0, 1] usando midrank (metade do peso para os empates).

    `percent_rank` do SQL devolveria 0.0 ou 1.0 degenerado quando ha muitos empates — o que
    e comum aqui, porque valores clipados nos extremos das bandas empatam com frequencia.
    O midrank distribui os empates e, com n=1, devolve 0.5 (neutro) em vez de um extremo.
    """
    if valor is None:
        return None
    amostra = [v for v in valores if v is not None]
    if not amostra:
        return None

    if melhor_maior:
        piores = sum(1 for v in amostra if v < valor)
    else:
        piores = sum(1 for v in amostra if v > valor)
    empatados = sum(1 for v in amostra if v == valor)

    return (piores + 0.5 * empatados) / len(amostra)


def perfil_de_pesos(linha: dict) -> str:
    return "financeiro" if linha.get("setor_financeiro") else "padrao"


def _macro_grupo(linha: dict) -> str:
    return "financeiro" if linha.get("setor_financeiro") else "nao_financeiro"


def escolher_grupo(linha: dict, universo: list[dict]) -> tuple[list[dict], str]:
    """Grupo de comparacao, em cascata do mais especifico ao mais amplo.

    Sempre restrito a mesma `fonte`: misturar ativos com dados reais e ativos sinteticos no
    mesmo ranking falsificaria o percentil dos dois lados.
    """
    elegiveis = [
        candidato
        for candidato in universo
        if candidato.get("elegivel_score") and candidato.get("fonte") == linha.get("fonte")
    ]

    setor = linha.get("setor")
    if setor:
        mesmo_setor = [c for c in elegiveis if c.get("setor") == setor]
        if len(mesmo_setor) >= MIN_GRUPO:
            return mesmo_setor, "setor"

    macro = _macro_grupo(linha)
    mesmo_macro = [c for c in elegiveis if _macro_grupo(c) == macro]
    if len(mesmo_macro) >= MIN_GRUPO:
        return mesmo_macro, "macro"

    return elegiveis, "universo"


def nota_metrica(nome: str, linha: dict, grupo: list[dict]) -> float | None:
    """Nota final do indicador: banda absoluta, ajustada pelo percentil quando ha amostra."""
    banda = nota_indicador(nome, linha.get(nome), linha)
    if banda is None:
        return None

    if len(grupo) < MIN_GRUPO:
        return banda

    valores = [c.get(nome) for c in grupo if c.get(nome) is not None]
    if len(valores) < MIN_GRUPO:
        return banda

    ruim, bom = BANDAS[nome]
    percentil = percentil_midrank(linha.get(nome), valores, melhor_maior=bom > ruim)
    if percentil is None:
        return banda

    return PESO_BANDA * banda + PESO_PERCENTIL * (percentil * 100.0)


def _media_ponderada(pares: list[tuple[float, float]], cobertura_minima: float):
    """Media ponderada sobre os itens disponiveis, renormalizando os pesos.

    Devolve (valor, peso_disponivel). O valor e None se o peso disponivel nao atingir o
    minimo — preferimos ausencia a um numero calculado sobre quase nada.
    """
    peso_total = sum(peso for peso, _ in pares if peso is not None)
    disponiveis = [(peso, nota) for peso, nota in pares if nota is not None]
    peso_disponivel = sum(peso for peso, _ in disponiveis)

    if peso_total <= 0:
        return None, 0.0

    fracao = peso_disponivel / peso_total
    if fracao < cobertura_minima or peso_disponivel <= 0:
        return None, fracao

    soma = sum(peso * nota for peso, nota in disponiveis)
    return soma / peso_disponivel, fracao


def notas_por_pilar(linha: dict, grupo: list[dict]) -> dict:
    perfil = perfil_de_pesos(linha)
    pesos = PESOS_METRICA[perfil]

    resultado = {}
    for pilar, metricas in pesos.items():
        pares = [
            (peso, nota_metrica(nome, linha, grupo)) for nome, peso in metricas.items()
        ]
        nota, _ = _media_ponderada(pares, COBERTURA_MINIMA_PILAR)
        resultado[pilar] = nota
    return resultado


def nota_letra(score) -> str:
    if score is None:
        return NOTA_SEM_DADOS
    for corte, letra in CORTES_NOTA:
        if score >= corte:
            return letra
    return "E"


def nivel_confianca(cobertura: float) -> str:
    if cobertura >= CONFIANCA_ALTA:
        return "alta"
    if cobertura >= CONFIANCA_MEDIA:
        return "media"
    return "baixa"


def flags_classicas(linha: dict) -> dict:
    """Formulas classicas de analise, independentes do score composto."""
    cotacao = linha.get("cotacao")
    lpa = linha.get("lpa")
    vpa = linha.get("vpa")
    dividend_yield = linha.get("dividend_yield")

    valor_graham = None
    margem_seguranca = None
    if lpa is not None and vpa is not None and lpa > 0 and vpa > 0:
        valor_graham = math.sqrt(CONSTANTE_GRAHAM * lpa * vpa)
        if cotacao:
            margem_seguranca = (valor_graham - cotacao) / cotacao

    # O preco-teto de Bazin depende do dividendo historico. Como o modulo `dividends` da
    # brapi e bloqueado no plano free, so calculamos quando o DY veio da propria API — um
    # yield estimado por aproximacao produziria um preco-teto sem significado.
    preco_teto_bazin = None
    if (
        dividend_yield is not None
        and cotacao is not None
        and linha.get("dividend_yield_origem") == "brapi"
    ):
        preco_teto_bazin = (cotacao * dividend_yield) / YIELD_ALVO_BAZIN

    liq_corrente = linha.get("liq_corrente")
    preco_lucro = linha.get("preco_lucro")
    preco_vp = linha.get("preco_valor_patrimonial")
    roe = linha.get("roe")
    endividamento = linha.get("divida_liquida_patrimonio")

    return {
        "valor_graham": valor_graham,
        "margem_seguranca": margem_seguranca,
        "flag_graham": None if margem_seguranca is None else margem_seguranca > 0,
        "preco_teto_bazin": preco_teto_bazin,
        "flag_bazin": (
            None
            if preco_teto_bazin is None or cotacao is None
            else cotacao < preco_teto_bazin
        ),
        "flag_pl_barato": (
            None if preco_lucro is None else 0 < preco_lucro <= 15
        ),
        "flag_pvp_barato": None if preco_vp is None else 0 < preco_vp <= 1.5,
        "flag_roe_bom": None if roe is None else roe >= 0.15,
        "flag_endiv_ok": None if endividamento is None else endividamento <= 1.0,
        "flag_liquidez_ok": None if liq_corrente is None else liq_corrente >= 1.5,
        "flag_dy_suspeito": (
            None if dividend_yield is None else dividend_yield > DY_SUSPEITO
        ),
    }


def sinal_tecnico(linha: dict) -> str:
    """Resume qualidade + preco num rotulo unico: FORTE, NEUTRO, FRACO ou SEM_DADOS.

    Nao e recomendacao de investimento — e a leitura do modelo, rastreavel ate os
    indicadores de origem (`score_final`, `margem_seguranca`, `confianca`).

    A assimetria das regras e intencional: para marcar FRACO basta UMA condicao ruim
    (score baixo OU preco muito acima do valor intrinseco), enquanto FORTE exige TODAS as
    condicoes boas ao mesmo tempo, inclusive confianca minima nos dados. Errar para o lado
    conservador e o comportamento correto aqui.
    """
    score = linha.get("score_final")
    if score is None or linha.get("elegivel_score") is False:
        return SINAL_SEM_DADOS

    margem = linha.get("margem_seguranca")

    # Qualquer sinal de alerta isolado ja rebaixa.
    if score < LIMITE_SCORE_FRACO:
        return SINAL_FRACO
    if margem is not None and margem < LIMITE_MARGEM_CARO:
        return SINAL_FRACO

    # FORTE exige score alto, preco abaixo do valor intrinseco e dados confiaveis. Quando a
    # margem de Graham nao e calculavel (prejuizo ou patrimonio negativo), nao bloqueamos o
    # sinal: o score ja incorpora isso, e exigir a margem puniria setores onde LPA/VPA nao
    # se aplicam.
    if (
        score >= LIMITE_SCORE_FORTE
        and linha.get("confianca") != "baixa"
        and (margem is None or margem > 0)
    ):
        return SINAL_FORTE

    return SINAL_NEUTRO


def _ranks(valores: list[float], crescente: bool) -> list[int]:
    """Ranking denso (empates compartilham a posicao), comecando em 1."""
    ordenados = sorted(set(valores), reverse=not crescente)
    posicao = {valor: i + 1 for i, valor in enumerate(ordenados)}
    return [posicao[v] for v in valores]


def aplicar_magic_formula(linhas: list[dict]) -> None:
    """Preenche `rank_magic_formula` in-place: soma do rank de EV/EBIT e do rank de ROIC.

    A Magic Formula de Joel Greenblatt combina "barato" (EV/EBIT baixo) com "bom negocio"
    (ROIC alto). Menor soma = melhor. So faz sentido entre ativos comparaveis, entao fica
    restrito aos elegiveis da mesma fonte.
    """
    for linha in linhas:
        linha.setdefault("rank_magic_formula", None)

    por_fonte: dict[str, list[dict]] = {}
    for linha in linhas:
        if not linha.get("elegivel_score"):
            continue
        if linha.get("ev_ebit") is None or linha.get("roic") is None:
            continue
        por_fonte.setdefault(linha.get("fonte"), []).append(linha)

    for candidatos in por_fonte.values():
        if len(candidatos) < 2:
            continue
        ranks_ev = _ranks([c["ev_ebit"] for c in candidatos], crescente=True)
        ranks_roic = _ranks([c["roic"] for c in candidatos], crescente=False)
        for candidato, r_ev, r_roic in zip(candidatos, ranks_ev, ranks_roic):
            candidato["rank_magic_formula"] = int(r_ev + r_roic)


def pontuar(linhas: list[dict]) -> list[dict]:
    """Calcula score, nota e flags para todo o universo.

    Nota de escala: com algumas dezenas de ativos, rodar isto em Python puro sobre uma lista
    e mais simples, mais rapido e infinitamente mais testavel que montar Window functions no
    Spark. Acima de ~10^5 ativos valeria migrar o percentil para `Window.percent_rank()`.
    """
    universo = list(linhas)
    resultados = []

    for linha in universo:
        saida = dict(linha)
        saida.update(flags_classicas(linha))

        if not linha.get("elegivel_score"):
            saida.update(
                {
                    "score_final": None,
                    "nota": NOTA_SEM_DADOS,
                    "score_valuation": None,
                    "score_rentabilidade": None,
                    "score_solidez": None,
                    "score_dividendos": None,
                    "grupo_usado": None,
                    "n_grupo": 0,
                    "perfil_pesos": perfil_de_pesos(linha),
                    "cobertura_indicadores_pct": 0.0,
                    "confianca": "baixa",
                }
            )
            resultados.append(saida)
            continue

        grupo, rotulo_grupo = escolher_grupo(linha, universo)
        pilares = notas_por_pilar(linha, grupo)

        score, cobertura = _media_ponderada(
            [(PESOS_PILAR[pilar], nota) for pilar, nota in pilares.items()],
            COBERTURA_MINIMA_SCORE,
        )

        saida.update(
            {
                "score_final": score,
                "nota": nota_letra(score),
                "score_valuation": pilares.get("valuation"),
                "score_rentabilidade": pilares.get("rentabilidade"),
                "score_solidez": pilares.get("solidez"),
                "score_dividendos": pilares.get("dividendos"),
                "grupo_usado": rotulo_grupo,
                "n_grupo": len(grupo),
                "perfil_pesos": perfil_de_pesos(linha),
                "cobertura_indicadores_pct": cobertura,
                "confianca": nivel_confianca(cobertura),
            }
        )
        resultados.append(saida)

    aplicar_magic_formula(resultados)
    return resultados
