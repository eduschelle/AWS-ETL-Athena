"""Fixtures compartilhadas pelos testes."""

import io
import json
from pathlib import Path

import pytest

DIRETORIO_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def payload_petr4() -> dict:
    """Resposta REAL da brapi.dev para PETR4, capturada e congelada.

    Usar um payload real como fixture e o que impede o mapeamento de ser testado apenas
    contra as suposicoes de quem o escreveu — varios campos que a documentacao sugere
    existirem vem nulos na pratica (totalAssets, revenuePerShare, lastDividendValue).
    """
    caminho = DIRETORIO_FIXTURES / "brapi_petr4.json"
    return json.loads(io.open(caminho, encoding="utf-8").read())


@pytest.fixture
def payload_banco() -> dict:
    """Payload tipico de instituicao financeira.

    Bancos nao tem lucro bruto, EBITDA nem capital de giro no sentido industrial, entao a
    brapi devolve `grossProfits`, `grossMargins`, `ebitda` e `currentRatio` nulos. Isso nao
    e erro de coleta: e a realidade do setor, e o pipeline precisa atravessar sem explodir.
    """
    return {
        "symbol": "ITUB4",
        "longName": "Itau Unibanco Holding SA",
        "regularMarketPrice": 33.80,
        "earningsPerShare": 3.80,
        "summaryProfile": {
            "sector": "Servicos Financeiros",
            "industry": "Bancos - Regional",
        },
        "defaultKeyStatistics": {
            "bookValue": 20.50,
            "sharesOutstanding": 9_800_000_000,
            "dividendYield": 0.055,
            "netIncomeToCommon": 37_000_000_000,
            "beta": 0.9,
            "enterpriseValue": None,
            "enterpriseToEbitda": None,
        },
        "financialData": {
            "totalRevenue": 190_000_000_000,
            "returnOnEquity": 0.185,
            "returnOnAssets": 0.015,
            "profitMargins": 0.195,
            "operatingMargins": None,
            "grossMargins": None,
            "grossProfits": None,
            "ebitda": None,
            "ebitdaMargins": None,
            "currentRatio": None,
            "totalDebt": None,
            "totalCash": None,
            "revenueGrowth": 0.08,
        },
    }


@pytest.fixture
def payload_bdr_vazio() -> dict:
    """BDR: recibo de empresa estrangeira. A brapi praticamente nao traz fundamentos."""
    return {
        "symbol": "MUTC34",
        "regularMarketPrice": 30.00,
        "summaryProfile": {},
        "defaultKeyStatistics": {},
        "financialData": {},
    }


def serie_sintetica(n: int, preco_inicial: float = 100.0) -> list[dict]:
    """Serie de pregoes deterministica, no formato de `historicalDataPrice`."""
    pontos = []
    preco = preco_inicial
    # Alterna +1% / -1% para produzir uma volatilidade estavel e previsivel.
    for i in range(n):
        preco = preco * (1.01 if i % 2 == 0 else 0.99)
        pontos.append(
            {
                "date": 1_700_000_000 + i * 86_400,
                "open": preco,
                "high": preco * 1.005,
                "low": preco * 0.995,
                "close": preco,
                "adjustedClose": preco,
                "volume": 1_000_000,
            }
        )
    return pontos
