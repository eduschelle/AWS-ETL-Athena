"""Testes da coercao de tipos na fronteira com o Spark.

O modulo `spark_comum` importa pyspark apenas dentro das funcoes que precisam de uma sessao, de
forma que a parte de preparacao de dados — que e onde os erros acontecem — seja testavel aqui.
"""

import math

import pytest

import esquemas
import spark_comum


def test_inteiros_sao_coagidos():
    """O Spark nao converte float em INT/BIGINT sozinho: 1.0 numa coluna INT e erro de tipo."""
    linhas = spark_comum.normalizar_linhas(
        [{"ticker": "PETR4", "volume": 31859600.0, "data_pregao": "2026-10-01"}],
        "precos_acoes",
    )
    assert isinstance(linhas[0]["volume"], int)
    assert linhas[0]["volume"] == 31859600


def test_nan_e_infinito_viram_nulo():
    """NaN em DOUBLE nao e NULL no Spark: passaria por isNotNull() e contaminaria medias."""
    linhas = spark_comum.normalizar_linhas(
        [
            {
                "ticker": "X",
                "abertura": float("nan"),
                "maxima": float("inf"),
                "minima": float("-inf"),
                "volume": float("nan"),
            }
        ],
        "precos_acoes",
    )
    assert linhas[0]["abertura"] is None
    assert linhas[0]["maxima"] is None
    assert linhas[0]["minima"] is None
    assert linhas[0]["volume"] is None


def test_campos_ausentes_viram_nulo_e_extras_sao_descartados():
    """Mudar o schema nunca deve produzir um erro obscuro no meio de um job."""
    linhas = spark_comum.normalizar_linhas(
        [{"ticker": "PETR4", "campo_que_nao_existe": 123}], "precos_acoes"
    )
    assert set(linhas[0]) == set(esquemas.colunas("precos_acoes"))
    assert "campo_que_nao_existe" not in linhas[0]
    assert linhas[0]["abertura"] is None


def test_ordem_das_chaves_segue_o_schema():
    linhas = spark_comum.normalizar_linhas([{"ticker": "PETR4"}], "precos_acoes")
    assert list(linhas[0]) == esquemas.colunas("precos_acoes")


def test_booleanos_e_textos():
    linhas = spark_comum.normalizar_linhas(
        [{"ticker": 123, "elegivel_score": 1, "setor_financeiro": 0}],
        "silver_indicadores_fundamentalistas",
    )
    assert linhas[0]["ticker"] == "123"
    assert linhas[0]["elegivel_score"] is True
    assert linhas[0]["setor_financeiro"] is False


def test_valor_nao_numerico_em_coluna_decimal_vira_nulo():
    linhas = spark_comum.normalizar_linhas(
        [{"ticker": "X", "abertura": "nao-e-numero"}], "precos_acoes"
    )
    assert linhas[0]["abertura"] is None


def test_evento_do_producer_atravessa_sem_perda():
    """Formato exato produzido por producer/producer.py."""
    evento = {
        "ticker": "PETR4",
        "data_pregao": "2026-10-01",
        "abertura": 38.50,
        "maxima": 39.10,
        "minima": 38.02,
        "fechamento": 38.95,
        "volume": 12_345_678,
        "evento_ts": "2026-10-01T12:00:00",
    }
    linha = spark_comum.normalizar_linhas([evento], "precos_acoes")[0]
    for chave, valor in evento.items():
        assert linha[chave] == valor


@pytest.mark.parametrize("tabela", sorted(esquemas.TABELAS))
def test_linha_vazia_nao_quebra_nenhuma_tabela(tabela):
    linha = spark_comum.normalizar_linhas([{}], tabela)[0]
    assert set(linha) == set(esquemas.colunas(tabela))
    assert all(valor is None for valor in linha.values())


def test_nenhum_nan_sobrevive_a_normalizacao():
    sujas = [{nome: float("nan") for nome in esquemas.colunas("gold_classificacao_ativos")}]
    linha = spark_comum.normalizar_linhas(sujas, "gold_classificacao_ativos")[0]
    for valor in linha.values():
        assert not (isinstance(valor, float) and math.isnan(valor))


# ---------------------------------------------------------------------------
# Resolucao de parametros do job
# ---------------------------------------------------------------------------
def test_data_coleta_vem_do_argumento(monkeypatch):
    monkeypatch.setenv("DATA_COLETA", "2020-01-01")
    # O argumento tem precedencia sobre o ambiente de proposito: `application_args` chega ao
    # job de forma garantida, enquanto `env_vars` depende de como o provider monta o
    # subprocesso do spark-submit.
    assert spark_comum.resolver_data_coleta(["--data-coleta", "2026-10-01"]) == "2026-10-01"
    assert spark_comum.resolver_data_coleta(["--data-coleta=2026-10-02"]) == "2026-10-02"


def test_data_coleta_cai_para_o_ambiente(monkeypatch):
    monkeypatch.setenv("DATA_COLETA", "2026-09-30")
    assert spark_comum.resolver_data_coleta([]) == "2026-09-30"


def test_data_coleta_cai_para_hoje(monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.delenv("DATA_COLETA", raising=False)
    hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert spark_comum.resolver_data_coleta([]) == hoje


def test_argumento_sem_valor_e_ignorado(monkeypatch):
    monkeypatch.setenv("DATA_COLETA", "2026-09-30")
    assert spark_comum.resolver_data_coleta(["--data-coleta"]) == "2026-09-30"


def test_fonte_entrada(monkeypatch):
    monkeypatch.delenv("FONTE_ENTRADA", raising=False)
    assert spark_comum.resolver_fonte_entrada([]) == "redis"
    assert spark_comum.resolver_fonte_entrada(["--fonte-entrada", "LANDING"]) == "landing"
    monkeypatch.setenv("FONTE_ENTRADA", "landing")
    assert spark_comum.resolver_fonte_entrada([]) == "landing"


def test_outros_argumentos_nao_confundem():
    argv = ["--outro", "x", "--data-coleta", "2026-10-01", "--mais", "y"]
    assert spark_comum.resolver_data_coleta(argv) == "2026-10-01"
