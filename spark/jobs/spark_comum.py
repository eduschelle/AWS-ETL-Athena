"""
Infraestrutura compartilhada pelos jobs Spark: sessao, coercao de tipos e escrita idempotente.

Os jobs deste projeto sao cascas finas de I/O — toda a regra de negocio vive nos modulos puros
(`brapi_mapeamento`, `pontuacao`, `categorias`), testaveis sem Spark. Este modulo e a cola.
"""

import logging
import math
import os
import sys
from datetime import datetime, timezone

import esquemas

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

log = logging.getLogger(__name__)


def _argumento(nome: str, argv: list[str] | None = None) -> str | None:
    """Le um argumento `--nome valor` ou `--nome=valor` da linha de comando."""
    argv = sys.argv[1:] if argv is None else argv
    for i, arg in enumerate(argv):
        if arg == f"--{nome}" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(f"--{nome}="):
            return arg.split("=", 1)[1]
    return None


def resolver_data_coleta(argv: list[str] | None = None) -> str:
    """Data de referencia do processamento: argumento de linha de comando, env, ou hoje (UTC).

    A precedencia do argumento sobre a variavel de ambiente e deliberada: o `env_vars` do
    SparkSubmitOperator depende de como o provider monta o ambiente do subprocesso, enquanto
    `application_args` chega ao job de forma garantida. A DAG passa os dois.
    """
    valor = _argumento("data-coleta", argv) or os.environ.get("DATA_COLETA", "").strip()
    return valor or datetime.now(timezone.utc).strftime("%Y-%m-%d")


def resolver_fonte_entrada(argv: list[str] | None = None) -> str:
    """"redis" consome a fila; "landing" reprocessa o JSON cru, sem HTTP e sem Redis."""
    valor = (
        _argumento("fonte-entrada", argv)
        or os.environ.get("FONTE_ENTRADA", "").strip()
        or "redis"
    )
    return valor.strip().lower()


def criar_sessao(nome_app: str):
    """Sessao Spark configurada para falar com o MinIO e sobrescrever particoes com seguranca."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(nome_app)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # CINTO E SUSPENSORIO. Esta config tambem e passada pelo SparkSubmitOperator, mas
        # precisa estar aqui tambem: sem ela, `mode("overwrite")` combinado com
        # `partitionBy` apaga o LOCATION INTEIRO da tabela, nao apenas a particao sendo
        # gravada. Se alguem rodar este job na mao sem a conf, perderia a tabela toda.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # 200 particoes de shuffle (default) para algumas dezenas de linhas produziria
        # centenas de arquivos minusculos.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _coagir(valor, tipo_logico: str):
    """Ajusta o valor Python ao tipo declarado no registro de schemas.

    Dois problemas resolvidos aqui:
    1. O Spark nao converte float em INT/BIGINT sozinho — 1.0 numa coluna INT e erro de tipo.
    2. NaN e +-Inf em coluna DOUBLE NAO sao NULL no Spark: passam por isNotNull(), contaminam
       avg() e envenenariam qualquer percentil ou media calculada depois.
    """
    if valor is None:
        return None

    if tipo_logico in ("inteiro", "longo"):
        try:
            if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
                return None
            return int(valor)
        except (TypeError, ValueError):
            return None

    if tipo_logico == "decimal":
        try:
            convertido = float(valor)
        except (TypeError, ValueError):
            return None
        if math.isnan(convertido) or math.isinf(convertido):
            return None
        return convertido

    if tipo_logico == "booleano":
        return None if valor is None else bool(valor)

    if tipo_logico == "texto":
        return valor if isinstance(valor, str) else str(valor)

    return valor


def normalizar_linhas(linhas: list[dict], tabela: str) -> list[dict]:
    """Projeta as linhas exatamente no schema da tabela: mesmas chaves, tipos coagidos.

    Campos ausentes viram None e campos extras sao descartados, de forma que mudar o schema
    nunca produza um erro obscuro de createDataFrame no meio de um job.
    """
    mapa_tipos = esquemas.tipos(tabela)
    return [
        {nome: _coagir(linha.get(nome), tipo) for nome, tipo in mapa_tipos.items()}
        for linha in linhas
    ]


def gravar_tabela(spark, linhas: list[dict], tabela: str, modo: str = "overwrite") -> int:
    """Grava linhas Python na tabela, particionando conforme o registro de schemas.

    Com `modo="overwrite"` e o overwrite dinamico ativo, reexecutar o job para a mesma
    particao substitui aquela particao em vez de duplicar linhas — e o que torna a DAG
    idempotente. Devolve a quantidade de linhas gravadas.
    """
    if not linhas:
        log.warning("Nada a gravar em %s.", tabela)
        return 0

    normalizadas = normalizar_linhas(linhas, tabela)
    df = spark.createDataFrame(normalizadas, schema=esquemas.ddl_spark(tabela))
    df = df.select(*esquemas.colunas(tabela))

    escritor = df.write.mode(modo)
    particoes = esquemas.colunas_particao(tabela)
    if particoes:
        escritor = escritor.partitionBy(*particoes)

    destino = esquemas.localizacao(tabela)
    escritor.parquet(destino)
    log.info("Gravadas %s linhas em %s (modo=%s).", len(normalizadas), destino, modo)
    return len(normalizadas)


def ler_particao(spark, tabela: str, coluna_particao: str, valor: str):
    """Le uma particao especifica, com descoberta de particao pelo caminho base.

    Lemos o location base (nao o diretorio da particao) de proposito: assim o Spark infere a
    coluna de particao a partir do caminho e ela aparece no DataFrame. Lendo o subdiretorio
    direto, a coluna simplesmente nao existiria.
    """
    from pyspark.sql.functions import col

    df = spark.read.parquet(esquemas.localizacao(tabela))
    return df.filter(col(coluna_particao) == valor)
