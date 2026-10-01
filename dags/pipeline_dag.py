"""
DAG: orquestra a pipeline de precos diarios de acoes da B3.

Redis (fila) -> Spark (transformacao) -> MinIO/S3 (Parquet particionado)
             -> Hive Metastore / Trino (catalogo consultavel via SQL)

O DDL da tabela nao vive mais aqui: ele e gerado por `esquemas.ddl_trino("precos_acoes")` e
aplicado por `catalogo_trino.registrar_tabela`. Antes, o mesmo schema era escrito a mao neste
arquivo e em `spark/jobs/spark_job.py`, com as colunas em ordens diferentes — e o conector Hive
do Trino casa colunas de Parquet por posicao, entao uma divergencia produziria dados trocados
em silencio, nao um erro.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import catalogo_trino

TABELA = "precos_acoes"

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
        python_callable=catalogo_trino.registrar_tabela,
        op_kwargs={"nome": TABELA},
    )

    processar_precos_com_spark >> atualizar_catalogo_trino
