"""
Producer: simula um feed de mercado enviando preços diários de ações da B3
para uma fila no Redis (equivalente a um produtor no Kinesis/SQS).

Como os dados não vêm de uma API real da B3 (a maioria exige cadastro/custo),
geramos preços sintéticos mas plausíveis: partimos de um preço-base aproximado
de cada ação e aplicamos uma variação percentual aleatória (random walk) a
cada evento, simulando abertura/máxima/mínima/fechamento/volume de um pregão.
"""

import json
import random
import time
from datetime import datetime

import redis

REDIS_HOST = "localhost"  # rodando fora do Docker; dentro da rede do compose seria "redis"
REDIS_PORT = 6379
REDIS_QUEUE = "b3:precos_acoes"

TICKERS = [
    "PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3",
    "B3SA3", "WEGE3", "MGLU3", "ITSA4", "BBAS3",
    "LEVE3", "MUTC34", "BRAV4", "EGIE3",
]

# Preço de fechamento "base" de cada ticker: ponto de partida do random walk
PRECOS_BASE = {
    "PETR4": 38.50,
    "VALE3": 61.20,
    "ITUB4": 33.80,
    "BBDC4": 14.90,
    "ABEV3": 12.40,
    "B3SA3": 11.60,
    "WEGE3": 41.30,
    "MGLU3": 2.15,
    "ITSA4": 9.85,
    "BBAS3": 27.40,
    "LEVE3": 24.00,
    "MUTC34": 30.00,
    "BRAV4": 20.00,
    "EGIE3": 40.00,
}


def gerar_evento(ticker: str, ultimo_fechamento: float, data_pregao: str) -> dict:
    variacao_dia = random.uniform(-0.03, 0.03)  # até 3% de variação no dia
    abertura = round(ultimo_fechamento * (1 + random.uniform(-0.01, 0.01)), 2)
    fechamento = round(abertura * (1 + variacao_dia), 2)
    maxima = round(max(abertura, fechamento) * (1 + random.uniform(0, 0.015)), 2)
    minima = round(min(abertura, fechamento) * (1 - random.uniform(0, 0.015)), 2)
    volume = random.randint(1_000_000, 50_000_000)

    return {
        "ticker": ticker,
        "data_pregao": data_pregao,
        "abertura": abertura,
        "maxima": maxima,
        "minima": minima,
        "fechamento": fechamento,
        "volume": volume,
        "evento_ts": datetime.utcnow().isoformat(),
    }


def main(qtd_eventos: int = 100, intervalo_segundos: float = 0.5) -> None:
    cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    precos_atuais = dict(PRECOS_BASE)
    data_pregao = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}. Enviando para a fila '{REDIS_QUEUE}'...")

    for i in range(qtd_eventos):
        ticker = random.choice(TICKERS)
        evento = gerar_evento(ticker, precos_atuais[ticker], data_pregao)
        precos_atuais[ticker] = evento["fechamento"]

        cliente.lpush(REDIS_QUEUE, json.dumps(evento))
        print(f"[{i + 1}/{qtd_eventos}] {ticker} -> fechamento={evento['fechamento']}")

        time.sleep(intervalo_segundos)

    print(f"Concluído. Tamanho atual da fila '{REDIS_QUEUE}': {cliente.llen(REDIS_QUEUE)}")


if __name__ == "__main__":
    main()
