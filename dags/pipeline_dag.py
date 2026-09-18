"""
DAG: orquestra a pipeline de precos diarios de acoes da B3.

Redis (fila) -> Spark (transformacao) -> MinIO/S3 (Parquet particionado)
             -> Hive Metastore / Trino (catalogo consultavel via SQL)
"""

from datetime import datetime

import trino
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_CATALOG = "hive"
TRINO_SCHEMA = "default"
TABELA = "precos_acoes"
LOCALIZACAO_S3 = "s3a://datalake/warehouse/precos_acoes/"


def atualizar_catalogo() -> None:
    """Garante que a tabela exista no catalogo e registra as novas partições gravadas pelo Spark."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cursor = conn.cursor()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABELA} (
            ticker VARCHAR,
            abertura DOUBLE,
            maxima DOUBLE,
            minima DOUBLE,
            fechamento DOUBLE,
            volume BIGINT,
            evento_ts VARCHAR,
            data_pregao VARCHAR
        )
        WITH (
            external_location = '{LOCALIZACAO_S3}',
            format = 'PARQUET',
            partitioned_by = ARRAY['data_pregao']
        )
        """
    )
    cursor.fetchall()

    cursor.execute(
        f"""
        CALL {TRINO_CATALOG}.system.sync_partition_metadata(
            schema_name => '{TRINO_SCHEMA}',
            table_name => '{TABELA}',
            mode => 'FULL'
        )
        """
    )
    cursor.fetchall()


default_args = {
    "owner": "eduschelle",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_b3_precos_acoes",
    description="ETL de precos diarios de acoes da B3: Redis -> Spark -> MinIO -> Hive/Trino",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["b3", "etl", "spark", "trino"],
) as dag:

    processar_precos_com_spark = SparkSubmitOperator(
        task_id="processar_precos_com_spark",
        application="/opt/airflow/spark_jobs/spark_job.py",
        conn_id="spark_default",
        name="b3-precos-acoes-etl",
        verbose=True,
    )

    atualizar_catalogo_trino = PythonOperator(
        task_id="atualizar_catalogo_trino",
        python_callable=atualizar_catalogo,
    )

    processar_precos_com_spark >> atualizar_catalogo_trino
