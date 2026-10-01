"""
Spark Job: drena a fila de eventos do Redis, monta um DataFrame e grava
no Data Lake (MinIO/S3) em Parquet, particionado por data_pregao.

Equivalente a um Job do AWS Glue / um Step de EMR.

O schema vem de `esquemas.ddl_spark("precos_acoes")`, a mesma fonte usada para gerar o DDL do
Trino — antes ele era um StructType escrito a mao aqui, duplicando o DDL da DAG.

NOTA SOBRE O MODO DE ESCRITA: este job usa `append` de proposito, diferente dos jobs de
fundamentos (bronze/silver/gold), que usam overwrite dinamico de particao. Aqui varios lotes de
eventos no mesmo dia sao semantica correta de streaming: a tabela e um feed de eventos, nao um
retrato diario unico. Trocar para overwrite apagaria os lotes anteriores do mesmo pregao.
"""

import json
import os

import redis

import esquemas
import spark_comum

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_QUEUE = os.environ.get("REDIS_QUEUE", "b3:precos_acoes")

TABELA = "precos_acoes"
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", esquemas.localizacao(TABELA))


def drenar_fila_redis() -> list[dict]:
    """Le e remove todos os eventos disponiveis na fila (RPOP = lado oposto ao LPUSH do producer)."""
    cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    eventos = []

    while True:
        bruto = cliente.rpop(REDIS_QUEUE)
        if bruto is None:
            break
        eventos.append(json.loads(bruto))

    return eventos


def main() -> None:
    eventos = drenar_fila_redis()

    if not eventos:
        print(f"Fila '{REDIS_QUEUE}' vazia. Nada para processar.")
        return

    print(f"{len(eventos)} eventos lidos do Redis. Iniciando processamento Spark...")

    spark = spark_comum.criar_sessao("b3-precos-acoes-etl")
    df = spark.createDataFrame(
        spark_comum.normalizar_linhas(eventos, TABELA),
        schema=esquemas.ddl_spark(TABELA),
    ).select(*esquemas.colunas(TABELA))

    (
        df.write.mode("append")
        .partitionBy(*esquemas.colunas_particao(TABELA))
        .parquet(OUTPUT_PATH)
    )

    print(f"Gravacao concluida em {OUTPUT_PATH}, particionado por data_pregao.")
    spark.stop()


if __name__ == "__main__":
    main()
