"""
Coleta de fundamentos na brapi.dev, executada por um PythonOperator dentro do Airflow.

Por que roda dentro do Airflow e nao no host como `producer/producer.py`: aquele producer
precisa do host porque simula um feed externo de mercado; uma chamada HTTP a uma API publica
nao tem esse motivo, e manter a coleta dentro do compose elimina a maior friccao operacional
do projeto (ter que rodar um script no WSL antes de disparar a DAG).

RESTRICOES DO PLANO FREE DA BRAPI QUE MOLDAM ESTE MODULO (verificadas na pratica):

- Maximo 1 ticker por requisicao e 1 requisicao CONCORRENTE. Nada de ThreadPoolExecutor:
  o loop e estritamente sequencial, de proposito.
- 15.000 requisicoes/mes. Com 14 tickers numa DAG semanal, o consumo e de ~60/mes (0.4%),
  entao a quota nao e o gargalo — o gargalo e o token ausente.
- Sem token, apenas PETR4, VALE3, ITUB4 e MGLU3 respondem; os outros devolvem
  HTTP 401 MISSING_TOKEN. Por isso NUNCA fazemos retry em 401/403/404: um retry cego em 401
  para 10 tickers x 3 tentativas queimaria quota por acidente, sem chance de sucesso.
- Uma unica requisicao com `range=3mo&interval=1d&modules=...` devolve fundamentos E 63
  pregoes de historico. A serie sai de graca; nunca vale uma segunda chamada.

PROTECAO DA QUOTA: o payload cru e gravado na landing do Data Lake ANTES de entrar na fila do
Redis. O drain da fila e destrutivo (RPOP), entao sem a landing uma falha do job Spark
posterior destruiria dados que custaram requisicoes. Com ela, bronze/silver/gold podem ser
reconstruidos quantas vezes for preciso sem um unico acesso HTTP.
"""

import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

URL_BASE = "https://brapi.dev/api/quote"
MODULOS = "defaultKeyStatistics,financialData,summaryProfile"
INTERVALO_HISTORICO = "3mo"
GRANULARIDADE_HISTORICO = "1d"

FILA_FUNDAMENTOS = os.environ.get("REDIS_QUEUE_FUNDAMENTOS", "b3:fundamentos")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")

# A landing fica FORA de warehouse/ de proposito: `sync_partition_metadata` falha ao
# encontrar diretorios que nao casem o padrao `coluna=valor` dentro do location de uma tabela.
PREFIXO_LANDING = "landing/fundamentos"

# Mesmo universo de `producer/producer.py`.
TICKERS_PADRAO = [
    "PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3",
    "B3SA3", "WEGE3", "MGLU3", "ITSA4", "BBAS3",
    "LEVE3", "MUTC34", "BRAV4", "EGIE3",
]

# Unicos tickers que a brapi atende sem token.
TICKERS_LIVRES = ["PETR4", "VALE3", "ITUB4", "MGLU3"]

# Setor e industria usados ao gerar fundamentos sinteticos. Vem no mesmo vocabulario em
# portugues que a brapi devolve, para que o agrupamento setorial do score funcione igual.
# Os rotulos de setor usam o MESMO vocabulario acentuado que a brapi devolve ("Energia",
# "Servicos Financeiros" -> "Servicos Financeiros"), confirmado contra a API para PETR4, VALE3,
# ITUB4 e MGLU3. Isso mantem a coluna `setor` consistente entre linhas reais e sinteticas.
PERFIL_SINTETICO = {
    "PETR4": (38.50, "Energia", "Petróleo e Gás Integrado"),
    "VALE3": (61.20, "Materiais Básicos", "Mineração - Ferro"),
    "ITUB4": (33.80, "Serviços Financeiros", "Bancos - Regional"),
    "BBDC4": (14.90, "Serviços Financeiros", "Bancos - Regional"),
    "ABEV3": (12.40, "Consumo Defensivo", "Bebidas - Cervejarias"),
    "B3SA3": (11.60, "Serviços Financeiros", "Bolsas e Mercados"),
    "WEGE3": (41.30, "Bens Industriais", "Equipamentos Elétricos"),
    "MGLU3": (2.15, "Consumo Cíclico", "Varejo Especializado"),
    "ITSA4": (9.85, "Serviços Financeiros", "Holdings Diversificadas"),
    "BBAS3": (27.40, "Serviços Financeiros", "Bancos - Regional"),
    "LEVE3": (24.00, "Consumo Cíclico", "Autopeças"),
    "MUTC34": (30.00, None, None),  # BDR: sem fundamentos, como na API real
    "BRAV4": (20.00, "Energia", "Petróleo e Gás - Exploração"),
    "EGIE3": (40.00, "Utilidade Pública", "Energia Elétrica"),
}

PREGOES_HISTORICO = 63
TENTATIVAS_MAXIMAS = 3
PAUSA_ENTRE_TICKERS = 0.4
# Status em que repetir a requisicao pode dar certo. 401/403/404 ficam de fora: repetir
# gasta quota sem nenhuma chance de sucesso.
STATUS_RETENTAVEIS = (408, 425, 429, 500, 502, 503, 504)

log = logging.getLogger(__name__)


class ColetaJaRealizada(Exception):
    """A landing do dia ja existe. O chamador deve pular a task, sem gastar requisicoes."""


# ---------------------------------------------------------------------------
# Universo de coleta
# ---------------------------------------------------------------------------
def resolver_universo(modo: str | None = None, token: str | None = None) -> list[tuple[str, str]]:
    """Decide quais tickers vem da API e quais serao sinteticos.

    Devolve pares (ticker, fonte) com fonte em {"brapi", "sintetico"}. A fonte viaja ate a
    tabela gold e e usada para impedir que ativos reais e sinteticos sejam rankeados no mesmo
    grupo de percentil — um ranking misturado falsificaria os dois lados.
    """
    modo = (modo or os.environ.get("BRAPI_MODO") or "auto").strip().lower()
    token = token if token is not None else os.environ.get("BRAPI_TOKEN", "")
    token = (token or "").strip()

    if modo == "sintetico":
        return [(t, "sintetico") for t in TICKERS_PADRAO]

    if modo == "livre":
        return [(t, "brapi") for t in TICKERS_LIVRES]

    if modo != "auto":
        raise ValueError(f"BRAPI_MODO invalido: {modo!r}. Use auto, livre ou sintetico.")

    if token:
        return [(t, "brapi") for t in TICKERS_PADRAO]

    # Modo degradado: o que a API entrega de graca vem real, o resto vem sintetico e
    # carimbado. Ao colar um token no .env, tudo vira "brapi" sem mudar uma linha de codigo.
    return [
        (t, "brapi" if t in TICKERS_LIVRES else "sintetico") for t in TICKERS_PADRAO
    ]


# ---------------------------------------------------------------------------
# Dados sinteticos
# ---------------------------------------------------------------------------
def _sorteio_estavel(ticker: str) -> random.Random:
    """Gerador semeado pelo ticker: a mesma empresa sempre recebe os mesmos fundamentos.

    Isso e deliberado e importante — torna o modo sintetico idempotente, de forma que
    reprocessar uma particao nao muda os numeros nem o ranking.
    """
    semente = int.from_bytes(hashlib.md5(ticker.encode("utf-8")).digest()[:4], "big")
    return random.Random(semente)


def gerar_sintetico(ticker: str, data_coleta: str) -> dict:
    """Payload no MESMO formato da brapi, para atravessar o mapeamento sem desvios.

    Gerar no formato da API (em vez de ja produzir indicadores prontos) e o que mantem o
    caminho sintetico e o caminho real exercitando exatamente o mesmo codigo de mapeamento.
    """
    rnd = _sorteio_estavel(ticker)
    preco_base, setor, industria = PERFIL_SINTETICO.get(ticker, (20.0, None, None))
    cotacao = round(preco_base * rnd.uniform(0.9, 1.1), 2)

    # BDRs nao tem fundamentos na brapi; reproduzir isso mantem o caminho de inelegibilidade
    # exercitado tambem no modo sintetico.
    if setor is None:
        return {
            "symbol": ticker,
            "regularMarketPrice": cotacao,
            "summaryProfile": {},
            "defaultKeyStatistics": {},
            "financialData": {},
            "historicalDataPrice": _historico_sintetico(ticker, cotacao),
        }

    acoes = rnd.randrange(500_000_000, 14_000_000_000)
    margem_liquida = round(rnd.uniform(0.04, 0.28), 4)
    margem_ebit = round(margem_liquida * rnd.uniform(1.2, 2.0), 4)
    receita = round(cotacao * acoes * rnd.uniform(0.4, 2.5), 0)
    lucro = round(receita * margem_liquida, 0)
    lpa = round(lucro / acoes, 6)
    roe = round(rnd.uniform(0.05, 0.30), 4)
    vpa = round(lpa / roe, 6) if roe else round(cotacao * 0.7, 6)
    financeiro = "financ" in (setor or "").lower() or "banco" in (industria or "").lower()

    return {
        "symbol": ticker,
        "longName": f"{ticker} Sintetico SA",
        "regularMarketPrice": cotacao,
        "earningsPerShare": lpa,
        "summaryProfile": {"sector": setor, "industry": industria},
        "defaultKeyStatistics": {
            "bookValue": vpa,
            "sharesOutstanding": acoes,
            "dividendYield": round(rnd.uniform(0.0, 0.11), 4),
            "netIncomeToCommon": lucro,
            "beta": round(rnd.uniform(0.4, 1.6), 4),
            "enterpriseToEbitda": None,
        },
        "financialData": {
            "totalRevenue": receita,
            "returnOnEquity": roe,
            "returnOnAssets": round(roe * rnd.uniform(0.2, 0.6), 4),
            "profitMargins": margem_liquida,
            # Bancos e seguradoras nao reportam estes campos — espelhar isso faz o perfil de
            # pesos "financeiro" ser exercitado de verdade no modo sintetico.
            "operatingMargins": None if financeiro else margem_ebit,
            "grossMargins": None if financeiro else round(rnd.uniform(0.2, 0.7), 4),
            "ebitda": None if financeiro else round(receita * margem_ebit * 1.3, 0),
            "ebitdaMargins": None if financeiro else round(margem_ebit * 1.3, 4),
            "currentRatio": None if financeiro else round(rnd.uniform(0.6, 3.0), 4),
            # Divida e caixa existem tambem para instituicoes financeiras (captacao), ao
            # contrario de lucro bruto, EBITDA e capital de giro. Bancos sao alavancados por
            # natureza, dai o multiplicador maior.
            "totalDebt": round(
                receita * rnd.uniform(1.5, 4.0) if financeiro else receita * rnd.uniform(0.2, 2.0),
                0,
            ),
            "totalCash": round(receita * rnd.uniform(0.05, 0.5), 0),
            "revenueGrowth": round(rnd.uniform(-0.10, 0.35), 4),
        },
        "historicalDataPrice": _historico_sintetico(ticker, cotacao),
    }


def _historico_sintetico(ticker: str, cotacao: float) -> list[dict]:
    """Random walk de PREGOES_HISTORICO dias, terminando proximo da cotacao informada."""
    rnd = _sorteio_estavel(ticker + "-serie")
    inicio = datetime.now(timezone.utc) - timedelta(days=PREGOES_HISTORICO)
    preco = cotacao * rnd.uniform(0.85, 1.15)
    pontos = []

    for i in range(PREGOES_HISTORICO):
        preco = max(0.5, preco * (1 + rnd.uniform(-0.03, 0.03)))
        dia = inicio + timedelta(days=i)
        fechamento = round(preco, 2)
        pontos.append(
            {
                "date": int(dia.timestamp()),
                "open": round(fechamento * rnd.uniform(0.99, 1.01), 2),
                "high": round(fechamento * rnd.uniform(1.0, 1.02), 2),
                "low": round(fechamento * rnd.uniform(0.98, 1.0), 2),
                "close": fechamento,
                "adjustedClose": fechamento,
                "volume": rnd.randrange(1_000_000, 60_000_000),
            }
        )
    return pontos


# ---------------------------------------------------------------------------
# Acesso HTTP
# ---------------------------------------------------------------------------
def coletar(ticker: str, token: str | None = None, sessao=None) -> tuple[dict | None, int]:
    """Busca um ticker na brapi. Devolve (payload, http_status).

    Uma unica requisicao traz fundamentos e historico. Em erro definitivo devolve
    (None, status) em vez de levantar excecao — um ticker sem permissao nao deve derrubar a
    coleta dos outros 13.
    """
    token = token if token is not None else os.environ.get("BRAPI_TOKEN", "")
    parametros = {
        "range": INTERVALO_HISTORICO,
        "interval": GRANULARIDADE_HISTORICO,
        "modules": MODULOS,
    }
    if token:
        parametros["token"] = token

    # `requests` so e importado quando ha de fato uma chamada de rede, para que os testes
    # possam injetar uma sessao falsa sem precisar da biblioteca instalada.
    if sessao is None:
        import requests

        sessao = requests
    cliente = sessao
    ultimo_status = 0

    for tentativa in range(1, TENTATIVAS_MAXIMAS + 1):
        try:
            resposta = cliente.get(
                f"{URL_BASE}/{ticker}", params=parametros, timeout=(5, 30)
            )
            ultimo_status = resposta.status_code

            if resposta.status_code == 200:
                corpo = resposta.json()
                resultados = corpo.get("results") or []
                if not resultados:
                    log.warning("%s: HTTP 200 sem results no corpo", ticker)
                    return None, 200
                return resultados[0], 200

            if resposta.status_code not in STATUS_RETENTAVEIS:
                # 401/403/404 caem aqui: insistir so gastaria quota.
                log.warning(
                    "%s: HTTP %s definitivo, sem retry (%s)",
                    ticker,
                    resposta.status_code,
                    resposta.text[:160],
                )
                return None, resposta.status_code

            log.warning(
                "%s: HTTP %s retentavel (tentativa %s/%s)",
                ticker,
                resposta.status_code,
                tentativa,
                TENTATIVAS_MAXIMAS,
            )
        except Exception as erro:  # timeouts e falhas de rede
            ultimo_status = -1
            log.warning(
                "%s: falha de rede (tentativa %s/%s): %s",
                ticker,
                tentativa,
                TENTATIVAS_MAXIMAS,
                erro,
            )

        if tentativa < TENTATIVAS_MAXIMAS:
            time.sleep(2 ** (tentativa - 1))

    return None, ultimo_status


# ---------------------------------------------------------------------------
# Landing no Data Lake
# ---------------------------------------------------------------------------
def _cliente_s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def caminho_landing(data_coleta: str, ticker: str) -> str:
    return f"{PREFIXO_LANDING}/data_coleta={data_coleta}/{ticker}.json"


def particao_ja_coletada(data_coleta: str, s3=None) -> bool:
    """Guard de quota: ja existe landing para esta data?"""
    s3 = s3 or _cliente_s3()
    resposta = s3.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=f"{PREFIXO_LANDING}/data_coleta={data_coleta}/",
        MaxKeys=1,
    )
    return resposta.get("KeyCount", 0) > 0


def gravar_landing(data_coleta: str, ticker: str, envelope: dict, s3=None) -> str:
    chave = caminho_landing(data_coleta, ticker)
    (s3 or _cliente_s3()).put_object(
        Bucket=MINIO_BUCKET,
        Key=chave,
        Body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return chave


def ler_landing(data_coleta: str, s3=None) -> list[dict]:
    """Le de volta a landing de um dia. E o modo replay, que nao gasta quota alguma."""
    s3 = s3 or _cliente_s3()
    prefixo = f"{PREFIXO_LANDING}/data_coleta={data_coleta}/"
    envelopes = []

    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=MINIO_BUCKET, Prefix=prefixo):
        for objeto in pagina.get("Contents") or []:
            if not objeto["Key"].endswith(".json"):
                continue
            corpo = s3.get_object(Bucket=MINIO_BUCKET, Key=objeto["Key"])["Body"].read()
            envelopes.append(json.loads(corpo.decode("utf-8")))

    return envelopes


# ---------------------------------------------------------------------------
# Orquestracao da coleta
# ---------------------------------------------------------------------------
def montar_envelope(ticker: str, payload, fonte: str, status: int, data_coleta: str) -> dict:
    """Linha do bronze: o payload cru preservado intacto, com metadados de coleta."""
    return {
        "ticker": ticker,
        "payload_json": json.dumps(payload, ensure_ascii=False) if payload else None,
        "http_status": int(status),
        "modulos_solicitados": MODULOS,
        "fonte": fonte,
        "coletado_em": datetime.now(timezone.utc).isoformat(),
        "data_coleta": data_coleta,
    }


def coletar_universo(
    modo: str | None = None,
    forcar_coleta: bool = False,
    data_coleta: str | None = None,
    **_,
) -> dict:
    """Ponto de entrada do PythonOperator. Devolve estatisticas da execucao."""
    import redis

    data_coleta = data_coleta or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = _cliente_s3()

    if not forcar_coleta and particao_ja_coletada(data_coleta, s3=s3):
        raise ColetaJaRealizada(
            f"Landing de {data_coleta} ja existe. Use forcar_coleta=True para recoletar."
        )

    universo = resolver_universo(modo)
    token = os.environ.get("BRAPI_TOKEN", "").strip()
    cliente_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    estatisticas = {
        "data_coleta": data_coleta,
        "requisicoes_http": 0,
        "reais": 0,
        "sinteticos": 0,
        "falhas": 0,
        "tickers_com_falha": [],
    }

    log.info(
        "Coletando %s tickers (%s reais, %s sinteticos) para data_coleta=%s",
        len(universo),
        sum(1 for _, f in universo if f == "brapi"),
        sum(1 for _, f in universo if f == "sintetico"),
        data_coleta,
    )

    for ticker, fonte in universo:
        if fonte == "brapi":
            payload, status = coletar(ticker, token=token)
            estatisticas["requisicoes_http"] += 1
            if payload is None:
                estatisticas["falhas"] += 1
                estatisticas["tickers_com_falha"].append(f"{ticker}:{status}")
            else:
                estatisticas["reais"] += 1
            # Pausa apenas entre chamadas de rede: o plano free permite 1 requisicao
            # concorrente, entao a coleta e sequencial por obrigacao, nao por simplicidade.
            time.sleep(PAUSA_ENTRE_TICKERS)
        else:
            payload, status = gerar_sintetico(ticker, data_coleta), 0
            estatisticas["sinteticos"] += 1

        envelope = montar_envelope(ticker, payload, fonte, status, data_coleta)
        # Landing ANTES da fila: o drain do Redis e destrutivo.
        gravar_landing(data_coleta, ticker, envelope, s3=s3)
        cliente_redis.lpush(FILA_FUNDAMENTOS, json.dumps(envelope, ensure_ascii=False))

    log.info("Coleta concluida: %s", estatisticas)
    if estatisticas["falhas"]:
        log.warning(
            "Tickers sem dados (provavel falta de token): %s",
            ", ".join(estatisticas["tickers_com_falha"]),
        )

    return estatisticas
