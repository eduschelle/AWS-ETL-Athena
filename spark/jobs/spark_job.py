"""
Spark Job: drena a fila de eventos do Redis, monta um DataFrame e grava
no Data Lake (MinIO/S3) em Parquet, particionado por data_pregao.

Equivalente a um Job do AWS Glue / um Step de EMR.
"""

import json
import os

import redis
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_QUEUE = os.environ.get("REDIS_QUEUE", "b3:precos_acoes")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "s3a://datalake/warehouse/precos_acoes/")

SCHEMA = StructType(
    [
        StructField("ticker", StringType(), nullable=False),
        StructField("data_pregao", StringType(), nullable=False),
        StructField("abertura", DoubleType(), nullable=False),
        StructField("maxima", DoubleType(), nullable=False),
        StructField("minima", DoubleType(), nullable=False),
        StructField("fechamento", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("evento_ts", StringType(), nullable=False),
    ]
)


def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("b3-precos-acoes-etl")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def drenar_fila_redis() -> list[dict]:
    """Lê e remove todos os eventos disponíveis na fila (RPOP = lado oposto ao LPUSH do producer)."""
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

    spark = criar_spark_session()
    df = spark.createDataFrame(eventos, schema=SCHEMA)

    (
        df.write.mode("append")
        .partitionBy("data_pregao")
        .parquet(OUTPUT_PATH)
    )

    print(f"Gravação concluída em {OUTPUT_PATH}, particionado por data_pregao.")
    spark.stop()


if __name__ == "__main__":
    main()
