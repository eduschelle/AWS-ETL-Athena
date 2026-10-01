"""
Traducao do payload da brapi.dev para os 22 indicadores do screener do Fundamentus.

Python puro (`dict -> dict`), sem pyspark e sem rede: toda a matematica financeira do
projeto vive aqui e e testavel isoladamente com pytest.

O QUE FOI VALIDADO CONTRA A API REAL (uma chamada a /api/quote/PETR4 sem token):

- O plano free entrega os 3 modulos (defaultKeyStatistics, financialData, summaryProfile)
  MAIS `historicalDataPrice` com 63 pregoes incluindo `adjustedClose`.
- `sector` e `industry` ja vem em portugues ("Energia", "Petroleo e Gas Integrado").
- `debtToEquity = 1.406` e RAZAO, nao percentual: confere exatamente com
  totalDebt / (bookValue * sharesOutstanding) = 676.283bi / 481.0bi. Isso valida de quebra
  a derivacao `patrimonio_liquido = bookValue * sharesOutstanding`, usada em varios lugares
  aqui, ja que `totalStockholderEquity` nao vem no plano free.
- Vem NULL: `totalAssets`, `revenuePerShare`, `currentPrice`, `lastDividendValue`. Os
  proxies abaixo nao sao refinamentos opcionais, sao obrigatorios.
- `dividendYield` VEM preenchido (fracao decimal: 0.07 = 7%).

- CAMPOS PRE-CALCULADOS ESTAO DEFASADOS em relacao a cotacao. No mesmo payload havia
  `marketCap` = 661.8bi na raiz vs 633.1bi em defaultKeyStatistics, e `priceEarnings` =
  4.75 vs `trailingPE` = 5.21. Os coerentes com `regularMarketPrice` (49.12) sao 633.1bi e
  4.75, porque 49.12 * 12.888.733.000 = 633.1bi e 49.12 / 10.3483 = 4.75.
  DAI A REGRA CENTRAL DESTE MODULO: razoes que envolvem preco sao RECALCULADAS a partir de
  `regularMarketPrice`, nunca lidas do campo pronto. Isso garante que todos os indicadores
  sejam internamente coerentes entre si e com a cotacao exibida.
"""

import math
from datetime import datetime, timezone

from categorias import (
    CLASSE_BDR,
    CLASSE_OUTRO,
    CLASSE_UNIT_OU_FII,
    classe_ativo_do_ticker,
    eh_setor_financeiro,
)

# Aliquota combinada IRPJ + CSLL no Brasil, usada para chegar ao EBIT pos-impostos do ROIC.
ALIQUOTA_IR = 0.34

PREGOES_POR_ANO = 252
# "Liq.2meses" do Fundamentus: ~42 pregoes uteis em dois meses.
PREGOES_LIQUIDEZ = 42
# Abaixo disto, volatilidade e liquidez media viram ruido — melhor NULL explicito.
MIN_PONTOS_SERIE = 20

MODULOS = "defaultKeyStatistics,financialData,summaryProfile"

# Limites de SANIDADE, nao de winsorizacao. Com n=14 um unico outlier definiria o proprio
# limite se usassemos percentis dos dados, entao os cortes sao fixos e de dominio: um valor
# fora daqui nao e um extremo legitimo, e erro de dado — vira NULL.
LIMITES_SANIDADE = {
    "preco_lucro": (-1000.0, 1000.0),
    "preco_valor_patrimonial": (-50.0, 100.0),
    "psr": (0.0, 100.0),
    "preco_ativo": (0.0, 100.0),
    "preco_ebit": (-1000.0, 1000.0),
    "ev_ebit": (-1000.0, 1000.0),
    "ev_ebitda": (-1000.0, 1000.0),
    "roe": (-5.0, 5.0),
    "roic": (-5.0, 5.0),
    "retorno_sobre_ativos": (-5.0, 5.0),
    "margem_bruta": (-10.0, 1.0),
    "margem_ebit": (-10.0, 1.0),
    "margem_liquida": (-10.0, 1.0),
    "margem_ebitda": (-10.0, 1.0),
    "liq_corrente": (0.0, 50.0),
    "divida_liquida_patrimonio": (-10.0, 50.0),
    "divida_bruta_patrimonio": (-10.0, 50.0),
    "divida_liquida_ebitda": (-50.0, 50.0),
    "dividend_yield": (0.0, 1.0),
    "cresc_receita_12m": (-1.0, 10.0),
    "volatilidade_anualizada": (0.0, 5.0),
}

# Indicadores considerados no calculo de cobertura. Sao os que o score consome; campos
# puramente informativos (beta, nome) nao contam para "tenho dados suficientes?".
CAMPOS_NUCLEO = (
    "preco_lucro",
    "preco_valor_patrimonial",
    "psr",
    "preco_ativo",
    "ev_ebit",
    "roe",
    "roic",
    "margem_liquida",
    "margem_ebit",
    "divida_liquida_patrimonio",
    "divida_liquida_ebitda",
    "liq_corrente",
    "dividend_yield",
)

# Classes de ativo para as quais os 22 indicadores do Fundamentus nao se aplicam: BDRs sao
# recibos de empresas estrangeiras (a brapi devolve quase nada) e FIIs nao tem lucro bruto,
# EBITDA nem patrimonio no sentido de uma empresa operacional.
CLASSES_INELEGIVEIS = (CLASSE_BDR, CLASSE_UNIT_OU_FII, CLASSE_OUTRO)


# ---------------------------------------------------------------------------
# Helpers numericos
# ---------------------------------------------------------------------------
def num(valor) -> float | None:
    """Converte para float, devolvendo None para nao-numeros, NaN e infinitos.

    NaN precisa morrer aqui: no Spark um NaN em coluna DOUBLE NAO e NULL, passa por
    isNotNull(), contamina avg() e envenenaria o percentil do grupo inteiro.
    """
    if valor is None or isinstance(valor, bool):
        return None
    try:
        convertido = float(valor)
    except (TypeError, ValueError):
        return None
    if math.isnan(convertido) or math.isinf(convertido):
        return None
    return convertido


def div_segura(a, b) -> float | None:
    """Divisao que nunca levanta excecao. Toda divisao deste modulo passa por aqui."""
    numerador, denominador = num(a), num(b)
    if numerador is None or denominador is None or denominador == 0:
        return None
    resultado = numerador / denominador
    return num(resultado)


def mult_segura(a, b) -> float | None:
    x, y = num(a), num(b)
    if x is None or y is None:
        return None
    return num(x * y)


def primeiro_valido(*valores) -> float | None:
    """Primeiro valor numerico valido da lista de fallbacks, na ordem dada."""
    for valor in valores:
        convertido = num(valor)
        if convertido is not None:
            return convertido
    return None


def aplicar_sanidade(nome: str, valor) -> float | None:
    """Descarta valores fora da faixa plausivel do indicador (vira NULL)."""
    convertido = num(valor)
    if convertido is None:
        return None
    faixa = LIMITES_SANIDADE.get(nome)
    if faixa is None:
        return convertido
    minimo, maximo = faixa
    if convertido < minimo or convertido > maximo:
        return None
    return convertido


def limpar_nao_finito(linha: dict) -> dict:
    """Converte NaN/+-Inf em None. Rodar sempre antes de entregar linhas ao Spark."""
    limpa = {}
    for chave, valor in linha.items():
        if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
            limpa[chave] = None
        else:
            limpa[chave] = valor
    return limpa


# ---------------------------------------------------------------------------
# Serie historica de precos
# ---------------------------------------------------------------------------
def _pontos_validos(historico) -> list[dict]:
    pontos = []
    for ponto in historico or []:
        if not isinstance(ponto, dict):
            continue
        fechamento = num(ponto.get("close"))
        if fechamento is None:
            continue
        pontos.append(ponto)
    return pontos


def metricas_de_serie(historico) -> dict:
    """Liquidez media e volatilidade anualizada a partir de `historicalDataPrice`.

    Esta serie vem na MESMA requisicao que os fundamentos (`range=3mo&interval=1d`), ou
    seja, a custo marginal zero de quota. Importante: estas metricas nao sao derivadas da
    tabela `precos_acoes` do projeto, que e um random walk sintetico — calcular
    "volatilidade" a partir dela produziria um numero que parece real e e ruido puro.
    """
    pontos = _pontos_validos(historico)
    n = len(pontos)
    resultado = {
        "liquidez_2meses": None,
        "volatilidade_anualizada": None,
        "pontos_serie": n,
        "liquidez_2meses_origem": "amostra_insuficiente",
    }

    if n < MIN_PONTOS_SERIE:
        return resultado

    # Liquidez: financeiro medio negociado por pregao nos ultimos ~2 meses.
    recentes = pontos[-PREGOES_LIQUIDEZ:]
    financeiros = [
        produto
        for produto in (
            mult_segura(p.get("close"), p.get("volume")) for p in recentes
        )
        if produto is not None
    ]
    if financeiros:
        resultado["liquidez_2meses"] = sum(financeiros) / len(financeiros)
        resultado["liquidez_2meses_origem"] = "serie_brapi_3mo"

    # Volatilidade: desvio-padrao amostral dos retornos diarios, anualizado.
    # Usa fechamento ajustado quando disponivel — proventos e splits criariam saltos
    # artificiais na serie nao-ajustada e inflariam a volatilidade.
    serie = [
        primeiro_valido(p.get("adjustedClose"), p.get("close")) for p in pontos
    ]
    serie = [v for v in serie if v is not None and v > 0]
    retornos = [
        serie[i] / serie[i - 1] - 1.0
        for i in range(1, len(serie))
        if serie[i - 1] > 0
    ]
    if len(retornos) >= MIN_PONTOS_SERIE - 1:
        media = sum(retornos) / len(retornos)
        variancia = sum((r - media) ** 2 for r in retornos) / (len(retornos) - 1)
        resultado["volatilidade_anualizada"] = aplicar_sanidade(
            "volatilidade_anualizada", math.sqrt(variancia) * math.sqrt(PREGOES_POR_ANO)
        )

    return resultado


def linhas_de_serie(ticker: str, historico, data_coleta: str) -> list[dict]:
    """Converte `historicalDataPrice` nas linhas da tabela `silver_precos_brapi`."""
    linhas = []
    for ponto in _pontos_validos(historico):
        epoch = num(ponto.get("date"))
        if epoch is None:
            continue
        data_pregao = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")
        volume = num(ponto.get("volume"))
        linhas.append(
            {
                "ticker": ticker,
                "abertura": num(ponto.get("open")),
                "maxima": num(ponto.get("high")),
                "minima": num(ponto.get("low")),
                "fechamento": num(ponto.get("close")),
                "fechamento_ajustado": num(ponto.get("adjustedClose")),
                "volume": int(volume) if volume is not None else None,
                "data_coleta": data_coleta,
                "data_pregao": data_pregao,
            }
        )
    return linhas


# ---------------------------------------------------------------------------
# Indicadores fundamentalistas
# ---------------------------------------------------------------------------
def extrair_indicadores(
    payload: dict,
    *,
    ticker: str | None = None,
    fonte: str = "brapi",
    coletado_em: str | None = None,
    data_coleta: str | None = None,
) -> dict:
    """Mapeia um `results[0]` da brapi para uma linha de `silver_indicadores_*`.

    Nunca levanta excecao por dado faltante: todo campo ausente vira None e a procedencia
    fica registrada nas colunas `*_origem`, para que um NULL no silver seja sempre
    distinguivel de um erro de processamento.
    """
    raiz = payload or {}
    dks = raiz.get("defaultKeyStatistics") or {}
    fd = raiz.get("financialData") or {}
    sp = raiz.get("summaryProfile") or {}

    papel = (ticker or raiz.get("symbol") or "").strip().upper()

    # --- insumos primarios ---
    cotacao = primeiro_valido(raiz.get("regularMarketPrice"), fd.get("currentPrice"))
    lpa = primeiro_valido(
        raiz.get("earningsPerShare"),
        dks.get("earningsPerShare"),
        dks.get("trailingEps"),
    )
    vpa = num(dks.get("bookValue"))
    acoes_emitidas = primeiro_valido(
        dks.get("sharesOutstanding"), dks.get("impliedSharesOutstanding")
    )

    # Recalculado a partir da cotacao: os dois `marketCap` do payload divergem entre si e
    # o da raiz esta defasado (ver docstring do modulo).
    valor_mercado = primeiro_valido(
        mult_segura(cotacao, acoes_emitidas), dks.get("marketCap"), raiz.get("marketCap")
    )
    # `totalStockholderEquity` nao existe no plano free; esta derivacao foi conferida
    # contra `debtToEquity` e bate exatamente.
    patrimonio_liquido = mult_segura(vpa, acoes_emitidas)

    receita_total = num(fd.get("totalRevenue"))
    margem_ebit = num(fd.get("operatingMargins"))
    # EBIT absoluto nao vem no plano free; reconstruido a partir da receita e da margem.
    ebit_estimado = mult_segura(receita_total, margem_ebit)
    ebitda = primeiro_valido(
        fd.get("ebitda"), mult_segura(receita_total, fd.get("ebitdaMargins"))
    )
    divida_total = num(fd.get("totalDebt"))
    caixa_total = num(fd.get("totalCash"))
    lucro_liquido = num(dks.get("netIncomeToCommon"))
    retorno_sobre_ativos = num(fd.get("returnOnAssets"))

    divida_liquida = None
    if divida_total is not None:
        divida_liquida = divida_total - (caixa_total or 0.0)

    # `totalAssets` vem NULL no plano free. Proxy algebrico: se ROA = lucro / ativos,
    # entao ativos = lucro / ROA. Validado no PETR4 (~1.28 trilhoes, ordem correta).
    ativo_total_estimado = div_segura(lucro_liquido, retorno_sobre_ativos)

    enterprise_value = num(dks.get("enterpriseValue"))
    if enterprise_value is None and valor_mercado is not None:
        enterprise_value = num(
            valor_mercado + (divida_total or 0.0) - (caixa_total or 0.0)
        )

    # --- os 22 do Fundamentus ---
    indicadores = {
        "cotacao": cotacao,
        # Recalculado; `trailingPE` do payload esta defasado em relacao a cotacao.
        "preco_lucro": div_segura(cotacao, lpa),
        "preco_valor_patrimonial": div_segura(cotacao, vpa),
        # `revenuePerShare` vem NULL, entao via valor de mercado / receita.
        "psr": div_segura(valor_mercado, receita_total),
        "dividend_yield": primeiro_valido(dks.get("dividendYield"), dks.get("yield")),
        "preco_ativo": div_segura(valor_mercado, ativo_total_estimado),
        # Exigiria ativo e passivo circulantes absolutos; `currentRatio` da apenas a razao
        # entre eles. Sem equivalente no plano free.
        "preco_capital_giro": None,
        "preco_ebit": div_segura(valor_mercado, ebit_estimado),
        # Exigiria `totalCurrentAssets`, ausente no plano free.
        "preco_ativo_circ_liquido": None,
        "ev_ebit": div_segura(enterprise_value, ebit_estimado),
        "ev_ebitda": primeiro_valido(
            dks.get("enterpriseToEbitda"), div_segura(enterprise_value, ebitda)
        ),
        "margem_bruta": primeiro_valido(
            fd.get("grossMargins"), div_segura(fd.get("grossProfits"), receita_total)
        ),
        # Deliberadamente NAO cai para `ebitdaMargins`: seria margem EBITDA disfarcada de
        # margem EBIT. O EBITDA vai para sua propria coluna.
        "margem_ebit": margem_ebit,
        "margem_liquida": primeiro_valido(
            fd.get("profitMargins"), dks.get("profitMargins")
        ),
        "liq_corrente": num(fd.get("currentRatio")),
        # Base diferente da do Fundamentus (que usa EBIT / (Ativos - Fornecedores - Caixa));
        # aqui: EBIT pos-IR sobre divida + patrimonio. Marcado em `roic_origem`.
        "roic": div_segura(
            mult_segura(ebit_estimado, 1.0 - ALIQUOTA_IR),
            None
            if patrimonio_liquido is None
            else (divida_total or 0.0) + patrimonio_liquido,
        ),
        "roe": primeiro_valido(
            fd.get("returnOnEquity"),
            dks.get("returnOnEquity"),
            div_segura(lucro_liquido, patrimonio_liquido),
        ),
        "liquidez_2meses": None,  # preenchido por metricas_de_serie
        "patrimonio_liquido": patrimonio_liquido,
        "divida_liquida_patrimonio": div_segura(divida_liquida, patrimonio_liquido),
        # `revenueGrowth` e crescimento anual sobre o trimestre, horizonte incompativel com
        # os 5 anos do Fundamentus. Fica NULL; o proxy de 12 meses vai em coluna propria.
        "cresc_receita_5a": None,
    }

    # --- derivadas extras, de graca no mesmo payload ---
    indicadores.update(
        {
            "lpa": lpa,
            "vpa": vpa,
            "valor_mercado": valor_mercado,
            "margem_ebitda": primeiro_valido(
                fd.get("ebitdaMargins"), div_segura(ebitda, receita_total)
            ),
            # ATENCAO: `debtToEquity` da brapi e divida BRUTA / patrimonio e vem como RAZAO
            # (verificado: 1.406 no PETR4), nao em percentual como na convencao do Yahoo.
            # Nao confundir com `divida_liquida_patrimonio`.
            "divida_bruta_patrimonio": primeiro_valido(
                fd.get("debtToEquity"), div_segura(divida_total, patrimonio_liquido)
            ),
            "divida_liquida_ebitda": div_segura(divida_liquida, ebitda),
            "cresc_receita_12m": num(fd.get("revenueGrowth")),
            "retorno_sobre_ativos": retorno_sobre_ativos,
            "volatilidade_anualizada": None,  # preenchido por metricas_de_serie
            "beta": num(dks.get("beta")),
            "enterprise_value": enterprise_value,
            "ebit_estimado": ebit_estimado,
            "ebitda": ebitda,
            "receita_total": receita_total,
            "divida_total": divida_total,
            "caixa_total": caixa_total,
            "lucro_liquido": lucro_liquido,
            "acoes_emitidas": acoes_emitidas,
            "ativo_total_estimado": ativo_total_estimado,
        }
    )

    # Sanidade aplicada na fronteira do silver: o payload cru fica preservado no bronze,
    # entao descartar aqui nao perde auditabilidade.
    for nome in list(indicadores):
        if nome in LIMITES_SANIDADE:
            indicadores[nome] = aplicar_sanidade(nome, indicadores[nome])

    setor = sp.get("sector") or None
    industria = sp.get("industry") or None

    linha = {
        "ticker": papel,
        "nome_longo": raiz.get("longName") or raiz.get("shortName") or None,
        "setor": setor,
        "industria": industria,
        **indicadores,
        "fonte": fonte,
        "classe_ativo": classe_ativo_do_ticker(papel),
        "setor_financeiro": eh_setor_financeiro(setor, industria),
        "lucro_negativo": lpa is not None and lpa <= 0,
        "pontos_serie": 0,
        "preco_ativo_origem": (
            "estimado_por_roa" if ativo_total_estimado is not None else "indisponivel"
        ),
        "roic_origem": (
            "ebit_pos_ir_sobre_capital"
            if indicadores["roic"] is not None
            else "indisponivel"
        ),
        "dividend_yield_origem": (
            "brapi" if indicadores["dividend_yield"] is not None else "indisponivel"
        ),
        "liquidez_2meses_origem": "amostra_insuficiente",
        "cresc_receita_origem": (
            "proxy_12m"
            if indicadores["cresc_receita_12m"] is not None
            else "indisponivel"
        ),
        "coletado_em": coletado_em,
        "data_coleta": data_coleta,
    }

    linha.update(metricas_de_serie(raiz.get("historicalDataPrice")))
    linha.update(classificar_elegibilidade(linha))
    return limpar_nao_finito(linha)


def classificar_elegibilidade(linha: dict) -> dict:
    """Decide se o ativo pode receber score, e registra o motivo quando nao pode."""
    preenchidos = sum(1 for campo in CAMPOS_NUCLEO if linha.get(campo) is not None)
    cobertura = preenchidos / len(CAMPOS_NUCLEO)

    motivo = None
    if linha.get("classe_ativo") in CLASSES_INELEGIVEIS:
        motivo = "classe_nao_suportada"
    elif linha.get("cotacao") is None:
        motivo = "sem_cotacao"
    elif cobertura < 0.30:
        motivo = "dados_insuficientes"

    return {
        "cobertura_campos_pct": cobertura,
        "elegivel_score": motivo is None,
        "motivo_inelegibilidade": motivo,
    }
