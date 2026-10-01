"""
DAG: sistema de classificacao de investimentos da B3, espelhando o screener do Fundamentus.

brapi.dev -> landing (JSON cru) -> Redis -> Spark (bronze/silver/gold) -> Hive/Trino

Separada de `pipeline_b3_precos_acoes` de proposito. Sao dois dominios com cadencias
diferentes: aquele e um feed de precos (diario), este e fundamento de empresa, que muda a cada
balanco trimestral. Rodar a coleta de fundamentos diariamente gastaria quota para reescrever
dados identicos — dai o agendamento SEMANAL.

`max_active_runs=1` nao e enfeite: o plano free da brapi permite 1 requisicao concorrente, e
duas execucoes simultaneas tambem competiriam pela mesma fila do Redis (comportamento ja
documentado no SKILL.md do projeto).
"""

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

import catalogo_trino
import coletor_brapi

DIRETORIO_JOBS = "/opt/airflow/spark_jobs"

# Os executores do Spark (container spark-worker) NAO tem o volume ./spark/jobs montado. Sem
# declarar py_files, o job *parece* funcionar — o driver roda em client mode dentro do
# airflow-scheduler e encontra os modulos via sys.path[0] — e so quebra com ModuleNotFoundError
# dentro do executor, no momento em que uma funcao pura e usada numa closure.
MODULOS_COMPARTILHADOS = [
    f"{DIRETORIO_JOBS}/esquemas.py",
    f"{DIRETORIO_JOBS}/spark_comum.py",
    f"{DIRETORIO_JOBS}/categorias.py",
    f"{DIRETORIO_JOBS}/brapi_mapeamento.py",
    f"{DIRETORIO_JOBS}/pontuacao.py",
    f"{DIRETORIO_JOBS}/coletor_brapi.py",
]

# Tres JVMs sequenciais em client mode dentro do airflow-scheduler, num Docker Desktop com 11
# containers: sem limite explicito, o driver reserva memoria demais. E 200 particoes de shuffle
# (o default) para ~14 linhas produziria centenas de arquivos minusculos.
CONF_SPARK = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "512m",
    "spark.sql.shuffle.partitions": "4",
    # Faz `mode("overwrite")` substituir APENAS a particao gravada. Sem isto, o overwrite
    # apagaria o location inteiro da tabela.
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

# Data de referencia da execucao, usada como valor de particao em todas as camadas.
#
# Usamos `data_interval_end` em vez de `ds` porque `ds` e o INICIO do intervalo: num
# agendamento semanal, apontaria para sete dias antes de a requisicao ter sido feita.
#
# Consequencia verificada na pratica, que vale conhecer: num disparo MANUAL o Airflow infere o
# intervalo a partir do cron, entao `data_interval_end` e o ultimo limite semanal COMPLETO (uma
# segunda-feira no passado), nao a data de hoje. Isso e desejavel aqui — um disparo ad hoc cai
# na mesma particao da execucao agendada da semana, em vez de poluir o historico com uma
# particao extra, e a combinacao com o guard de quota do coletor faz o redisparo recalcular
# silver e gold sem gastar nenhuma requisicao. Para forcar uma releitura da API na particao
# corrente, use o parametro `forcar_coleta`.
DATA_REFERENCIA = "{{ data_interval_end | ds }}"


def coletar_fundamentos(data_coleta: str, **contexto) -> dict:
    """Coleta na brapi, grava a landing e publica na fila. Pula se o dia ja foi coletado."""
    parametros = contexto.get("params") or {}
    try:
        return coletor_brapi.coletar_universo(
            modo=parametros.get("modo_coleta"),
            forcar_coleta=bool(parametros.get("forcar_coleta")),
            data_coleta=data_coleta,
        )
    except coletor_brapi.ColetaJaRealizada as motivo:
        # Guard de quota: nao foi feita nenhuma requisicao HTTP. As tasks seguintes continuam,
        # reprocessando bronze/silver/gold a partir do que ja esta no Data Lake.
        raise AirflowSkipException(str(motivo)) from motivo


def job_spark(task_id: str, arquivo: str, **extras) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{DIRETORIO_JOBS}/{arquivo}",
        conn_id="spark_default",
        name=f"b3-{task_id.replace('_', '-')}",
        py_files=",".join(MODULOS_COMPARTILHADOS),
        conf=CONF_SPARK,
        verbose=True,
        **extras,
    )


def registrar(task_id: str, tabela: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=catalogo_trino.registrar_tabela,
        op_kwargs={"nome": tabela},
    )


default_args = {
    "owner": "eduschelle",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_b3_classificacao_investimentos",
    description=(
        "Classificacao de investimentos da B3 (score + categoria) a partir de fundamentos "
        "da brapi.dev, no formato do screener do Fundamentus"
    ),
    default_args=default_args,
    schedule="0 6 * * 1",  # toda segunda-feira as 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        # Recoleta mesmo que a landing do dia ja exista (gasta quota de proposito).
        "forcar_coleta": False,
        # auto = token se houver, senao 4 tickers livres + o resto sintetico.
        "modo_coleta": "auto",
        # "redis" consome a fila; "landing" reprocessa o JSON cru do dia sem nenhum HTTP.
        "fonte_entrada": "redis",
    },
    tags=["b3", "fundamentos", "classificacao", "spark", "trino"],
) as dag:

    coletar = PythonOperator(
        task_id="coletar_fundamentos_brapi",
        python_callable=coletar_fundamentos,
        op_kwargs={"data_coleta": DATA_REFERENCIA},
    )

    processar_bronze = job_spark(
        "processar_bronze",
        "job_bronze_fundamentos.py",
        application_args=[
            "--data-coleta", DATA_REFERENCIA,
            "--fonte-entrada", "{{ params.fonte_entrada }}",
        ],
        # NONE_FAILED (em vez do all_success padrao) para que uma coleta PULADA nao bloqueie o
        # resto: redisparar a DAG no mesmo dia recalcula silver e gold a partir do bronze ja
        # existente, sem gastar uma unica requisicao.
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    processar_silver = job_spark(
        "processar_silver",
        "job_silver_indicadores.py",
        application_args=["--data-coleta", DATA_REFERENCIA],
    )

    processar_gold = job_spark(
        "processar_gold",
        "job_gold_classificacao.py",
        application_args=["--data-coleta", DATA_REFERENCIA],
    )

    registrar_bronze = registrar("registrar_bronze", "bronze_fundamentos")
    registrar_indicadores = registrar(
        "registrar_silver_indicadores", "silver_indicadores_fundamentalistas"
    )
    registrar_precos = registrar("registrar_silver_precos", "silver_precos_brapi")
    registrar_gold = registrar("registrar_gold", "gold_classificacao_ativos")

    criar_view = PythonOperator(
        task_id="criar_view_screener_fundamentus",
        python_callable=catalogo_trino.criar_view_screener,
    )

    (
        coletar
        >> processar_bronze
        >> registrar_bronze
        >> processar_silver
        >> [registrar_indicadores, registrar_precos]
        >> processar_gold
        >> registrar_gold
        >> criar_view
    )
