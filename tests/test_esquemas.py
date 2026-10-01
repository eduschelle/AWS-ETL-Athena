"""Testes do registro de schemas — em especial a regressao do refactor de `precos_acoes`."""

import re

import pytest

import esquemas

# DDL exatamente como estava escrito a mao em dags/pipeline_dag.py antes do refactor.
# Este literal e o contrato: a tabela `precos_acoes` tem dados reais ja gravados no MinIO, e
# o conector Hive do Trino casa colunas de Parquet por posicao. Se o gerador passar a emitir
# uma ordem diferente, os Parquets existentes viram lixo silencioso — nao um erro.
DDL_HISTORICO_PRECOS_ACOES = """
CREATE TABLE IF NOT EXISTS hive.default.precos_acoes (
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
    external_location = 's3a://datalake/warehouse/precos_acoes/',
    format = 'PARQUET',
    partitioned_by = ARRAY['data_pregao']
)
"""

# Ordem fisica das colunas DENTRO do arquivo Parquet de precos_acoes, como gravada hoje pelo
# spark_job.py (o partitionBy remove data_pregao do arquivo).
ORDEM_FISICA_PRECOS_ACOES = [
    "ticker",
    "abertura",
    "maxima",
    "minima",
    "fechamento",
    "volume",
    "evento_ts",
]


def normalizar(sql: str) -> str:
    """Colapsa espacos em branco para comparar SQL por conteudo, nao por formatacao."""
    return re.sub(r"\s+", " ", sql).strip()


def test_ddl_precos_acoes_nao_mudou():
    assert normalizar(esquemas.ddl_trino("precos_acoes")) == normalizar(
        DDL_HISTORICO_PRECOS_ACOES
    )


def test_ordem_fisica_de_precos_acoes_preservada():
    assert esquemas.colunas_dados("precos_acoes") == ORDEM_FISICA_PRECOS_ACOES


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_particoes_sempre_ao_final(tabela):
    """Invariante central: o Hive exige colunas de particao no fim do DDL."""
    todas = esquemas.colunas(tabela)
    particoes = esquemas.colunas_particao(tabela)
    assert particoes, f"{tabela} deveria ter ao menos uma coluna de particao"
    assert todas[-len(particoes) :] == particoes


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_sem_colunas_duplicadas(tabela):
    nomes = esquemas.colunas(tabela)
    assert len(nomes) == len(set(nomes))


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_ddl_spark_cobre_todas_as_colunas(tabela):
    """O DDL do Spark inclui as colunas de particao: e o partitionBy que as remove."""
    ddl = esquemas.ddl_spark(tabela)
    campos = [parte.strip().split()[0] for parte in ddl.split(",")]
    assert campos == esquemas.colunas(tabela)


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_ddl_trino_declara_particao_e_localizacao(tabela):
    ddl = esquemas.ddl_trino(tabela)
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS hive.default.{tabela} (")
    assert f"external_location = '{esquemas.localizacao(tabela)}'" in ddl
    assert "format = 'PARQUET'" in ddl
    for particao in esquemas.colunas_particao(tabela):
        assert f"'{particao}'" in ddl.split("partitioned_by")[1]


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_localizacao_dentro_do_warehouse(tabela):
    """A landing de JSON cru NAO pode cair sob o prefixo de uma tabela.

    `sync_partition_metadata` falha ao encontrar diretorios que nao casem `coluna=valor`
    dentro do location da tabela — por isso a landing vive em s3a://datalake/landing/.
    """
    localizacao = esquemas.localizacao(tabela)
    assert localizacao.startswith("s3a://datalake/warehouse/")
    assert localizacao.endswith("/")
    assert localizacao.endswith(f"/{tabela}/")


def test_tabela_desconhecida_da_erro_explicativo():
    with pytest.raises(KeyError, match="nao registrada"):
        esquemas.ddl_trino("tabela_que_nao_existe")
