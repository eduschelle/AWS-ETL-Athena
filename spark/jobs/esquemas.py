"""
Registro unico dos schemas das tabelas do Data Lake.

Por que este modulo existe: antes dele, o schema de `precos_acoes` era escrito a mao em
DOIS lugares — o StructType do PySpark em `spark_job.py` e o DDL do Trino em
`pipeline_dag.py` — e as colunas apareciam em ordens diferentes nos dois. Funcionava
apenas por coincidencia: `partitionBy` remove a coluna de particao do arquivo Parquet, e
a ordem fisica resultante batia com a do DDL. Como o conector Hive do Trino casa colunas
de Parquet por POSICAO (default), qualquer reordenacao em um dos arquivos sem espelhar no
outro produziria dados silenciosamente trocados, nao um erro.

Aqui cada tabela e declarada uma unica vez e os dois dialetos sao GERADOS.

Nota deliberada: o schema e guardado como string de DDL, nao como StructType, para que
este modulo NAO importe pyspark. Dois motivos: o parser de DAGs do Airflow reimporta este
arquivo a cada poucos segundos (importar pyspark ali seria custo puro), e assim os testes
rodam no host sem pyspark instalado. `spark.createDataFrame(dados, schema="...")` aceita
string de DDL normalmente.
"""

CATALOGO = "hive"
SCHEMA = "default"

# Tipo logico -> (tipo no Trino, tipo no DDL do Spark)
TIPOS = {
    "texto": ("VARCHAR", "STRING"),
    "decimal": ("DOUBLE", "DOUBLE"),
    "inteiro": ("INTEGER", "INT"),
    "longo": ("BIGINT", "BIGINT"),
    "booleano": ("BOOLEAN", "BOOLEAN"),
}


def _col(nome: str, tipo: str, particao: bool = False) -> dict:
    if tipo not in TIPOS:
        raise ValueError(f"Tipo logico desconhecido: {tipo!r}")
    return {"nome": nome, "tipo": tipo, "particao": particao}


def _decimais(*nomes: str) -> list[dict]:
    """Atalho: a maioria esmagadora das colunas de indicador e DOUBLE."""
    return [_col(n, "decimal") for n in nomes]


# ---------------------------------------------------------------------------
# Tabelas
# ---------------------------------------------------------------------------
# IMPORTANTE: as colunas nao-particionadas devem ser declaradas na ordem FISICA em que o
# Spark as grava no Parquet. As colunas de particao vao sempre por ultimo (exigencia do
# Hive) e sao emitidas no fim do DDL automaticamente por `ddl_trino`.

TABELAS: dict[str, dict] = {
    # Tabela pre-existente do pipeline de precos sinteticos. A ordem abaixo reproduz
    # exatamente a ordem fisica ja gravada no MinIO — nao reordenar, os Parquets
    # existentes se tornariam ilegiveis.
    "precos_acoes": {
        "localizacao": "s3a://datalake/warehouse/precos_acoes/",
        "colunas": [
            _col("ticker", "texto"),
            _col("abertura", "decimal"),
            _col("maxima", "decimal"),
            _col("minima", "decimal"),
            _col("fechamento", "decimal"),
            _col("volume", "longo"),
            _col("evento_ts", "texto"),
            _col("data_pregao", "texto", particao=True),
        ],
    },
    # Bronze: resposta HTTP crua, sem tipagem. A brapi devolve null em metade dos campos
    # de forma imprevisivel por tipo de ativo (banco != BDR != ETF); tipar aqui
    # transformaria qualquer mudanca na API em job quebrado. Schema-on-read.
    "bronze_fundamentos": {
        "localizacao": "s3a://datalake/warehouse/bronze_fundamentos/",
        "colunas": [
            _col("ticker", "texto"),
            _col("payload_json", "texto"),
            _col("http_status", "inteiro"),
            _col("modulos_solicitados", "texto"),
            _col("fonte", "texto"),
            _col("coletado_em", "texto"),
            _col("data_coleta", "texto", particao=True),
        ],
    },
    # Silver: os 22 indicadores do Fundamentus + derivadas + metadados de procedencia.
    # Chave logica: (ticker, data_coleta).
    "silver_indicadores_fundamentalistas": {
        "localizacao": "s3a://datalake/warehouse/silver_indicadores_fundamentalistas/",
        "colunas": [
            # --- identificacao ---
            _col("ticker", "texto"),
            _col("nome_longo", "texto"),
            _col("setor", "texto"),
            _col("industria", "texto"),
            # --- os 22 do screener do Fundamentus ---
            *_decimais(
                "cotacao",
                "preco_lucro",
                "preco_valor_patrimonial",
                "psr",
                "dividend_yield",
                "preco_ativo",
                "preco_capital_giro",  # indisponivel no plano free -> sempre NULL
                "preco_ebit",
                "preco_ativo_circ_liquido",  # indisponivel no plano free -> sempre NULL
                "ev_ebit",
                "ev_ebitda",
                "margem_bruta",
                "margem_ebit",
                "margem_liquida",
                "liq_corrente",
                "roic",
                "roe",
                "liquidez_2meses",
                "patrimonio_liquido",
                "divida_liquida_patrimonio",
                "cresc_receita_5a",  # indisponivel no plano free -> sempre NULL
            ),
            # --- derivadas que saem de graca do mesmo payload ---
            *_decimais(
                "lpa",
                "vpa",
                "valor_mercado",
                "margem_ebitda",
                "divida_bruta_patrimonio",
                "divida_liquida_ebitda",
                "cresc_receita_12m",
                "retorno_sobre_ativos",
                "volatilidade_anualizada",
                "beta",
                # insumos brutos, preservados para auditoria dos indicadores derivados
                "enterprise_value",
                "ebit_estimado",
                "ebitda",
                "receita_total",
                "divida_total",
                "caixa_total",
                "lucro_liquido",
                "acoes_emitidas",
                "ativo_total_estimado",
            ),
            # --- metadados de procedencia: sem eles um NULL e indistinguivel de erro ---
            _col("fonte", "texto"),
            _col("classe_ativo", "texto"),
            _col("setor_financeiro", "booleano"),
            _col("elegivel_score", "booleano"),
            _col("motivo_inelegibilidade", "texto"),
            _col("lucro_negativo", "booleano"),
            _col("cobertura_campos_pct", "decimal"),
            _col("pontos_serie", "inteiro"),
            _col("preco_ativo_origem", "texto"),
            _col("roic_origem", "texto"),
            _col("dividend_yield_origem", "texto"),
            _col("liquidez_2meses_origem", "texto"),
            _col("cresc_receita_origem", "texto"),
            _col("coletado_em", "texto"),
            _col("data_coleta", "texto", particao=True),
        ],
    },
    # Serie historica REAL vinda da brapi. Nomes de coluna propositalmente iguais aos de
    # `precos_acoes` (permite UNION ALL didatico real vs. sintetico), mas tabela separada
    # para nao contaminar o dataset sintetico com o real nem vice-versa.
    "silver_precos_brapi": {
        "localizacao": "s3a://datalake/warehouse/silver_precos_brapi/",
        "colunas": [
            _col("ticker", "texto"),
            _col("abertura", "decimal"),
            _col("maxima", "decimal"),
            _col("minima", "decimal"),
            _col("fechamento", "decimal"),
            _col("fechamento_ajustado", "decimal"),
            _col("volume", "longo"),
            _col("data_coleta", "texto"),
            _col("data_pregao", "texto", particao=True),
        ],
    },
    # Gold: snapshot acumulativo. Particionada por data_referencia e gravada com overwrite
    # DINAMICO (so a particao corrente), preservando o historico — a evolucao do score ao
    # longo do tempo custa ~14 linhas por execucao e habilita LAG().
    "gold_classificacao_ativos": {
        "localizacao": "s3a://datalake/warehouse/gold_classificacao_ativos/",
        "colunas": [
            _col("ticker", "texto"),
            _col("cotacao", "decimal"),
            # --- score ---
            _col("score_final", "decimal"),
            _col("nota", "texto"),
            _col("score_valuation", "decimal"),
            _col("score_rentabilidade", "decimal"),
            _col("score_solidez", "decimal"),
            _col("score_dividendos", "decimal"),
            # --- como o score foi calculado (auditabilidade) ---
            _col("grupo_usado", "texto"),
            _col("n_grupo", "inteiro"),
            _col("perfil_pesos", "texto"),
            _col("cobertura_indicadores_pct", "decimal"),
            _col("confianca", "texto"),
            # --- categorias ---
            _col("setor", "texto"),
            _col("industria", "texto"),
            _col("classe_ativo", "texto"),
            _col("porte", "texto"),
            _col("perfil_risco", "texto"),
            _col("perfil_investidor", "texto"),
            # --- metricas de apoio ---
            _col("valor_mercado", "decimal"),
            _col("volatilidade_anualizada", "decimal"),
            _col("liquidez_2meses", "decimal"),
            # --- formulas classicas ---
            _col("valor_graham", "decimal"),
            _col("margem_seguranca", "decimal"),
            _col("flag_graham", "booleano"),
            _col("preco_teto_bazin", "decimal"),
            _col("flag_bazin", "booleano"),
            _col("rank_magic_formula", "inteiro"),
            # --- flags de screening ---
            _col("flag_pl_barato", "booleano"),
            _col("flag_pvp_barato", "booleano"),
            _col("flag_roe_bom", "booleano"),
            _col("flag_endiv_ok", "booleano"),
            _col("flag_liquidez_ok", "booleano"),
            _col("flag_dy_suspeito", "booleano"),
            # --- procedencia ---
            _col("elegivel_score", "booleano"),
            _col("fonte", "texto"),
            _col("data_referencia", "texto", particao=True),
        ],
    },
}


def _tabela(nome: str) -> dict:
    try:
        return TABELAS[nome]
    except KeyError:
        raise KeyError(
            f"Tabela {nome!r} nao registrada. Conhecidas: {sorted(TABELAS)}"
        ) from None


def _ordenadas(nome: str) -> list[dict]:
    """Colunas na ordem canonica: dados na ordem fisica, depois as de particao.

    Esta ordenacao e o invariante central do modulo — o Hive exige colunas de particao ao
    final, e o Spark grava o Parquet na ordem do schema menos as de particao.
    """
    cols = _tabela(nome)["colunas"]
    return [c for c in cols if not c["particao"]] + [c for c in cols if c["particao"]]


def colunas(nome: str) -> list[str]:
    """Todos os nomes de coluna, nao-particionadas primeiro e particoes ao final."""
    return [c["nome"] for c in _ordenadas(nome)]


def colunas_particao(nome: str) -> list[str]:
    return [c["nome"] for c in _tabela(nome)["colunas"] if c["particao"]]


def colunas_dados(nome: str) -> list[str]:
    """Colunas que realmente vao para dentro do arquivo Parquet (sem as de particao)."""
    return [c["nome"] for c in _tabela(nome)["colunas"] if not c["particao"]]


def tipos(nome: str) -> dict[str, str]:
    """Mapa coluna -> tipo logico. Usado para coagir valores antes de entregar ao Spark.

    Necessario porque o Spark nao converte float em INT/BIGINT sozinho: um valor 1.0 numa
    coluna declarada INT faz o createDataFrame falhar com erro de tipo.
    """
    return {c["nome"]: c["tipo"] for c in _ordenadas(nome)}


def localizacao(nome: str) -> str:
    return _tabela(nome)["localizacao"]


def nome_completo(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def ddl_spark(nome: str) -> str:
    """String de DDL para `spark.createDataFrame(dados, schema=...)`.

    Inclui as colunas de particao: e o `partitionBy` na escrita que as remove do arquivo.
    """
    return ", ".join(f"{c['nome']} {TIPOS[c['tipo']][1]}" for c in _ordenadas(nome))


def ddl_trino(nome: str) -> str:
    """CREATE TABLE IF NOT EXISTS completo, pronto para o cliente Python do Trino."""
    linhas = ",\n".join(
        f"    {c['nome']} {TIPOS[c['tipo']][0]}" for c in _ordenadas(nome)
    )

    propriedades = [
        f"    external_location = '{localizacao(nome)}'",
        "    format = 'PARQUET'",
    ]
    particoes = colunas_particao(nome)
    if particoes:
        lista = ", ".join(f"'{p}'" for p in particoes)
        propriedades.append(f"    partitioned_by = ARRAY[{lista}]")
    corpo_propriedades = ",\n".join(propriedades)

    return (
        f"CREATE TABLE IF NOT EXISTS {nome_completo(nome)} (\n"
        f"{linhas}\n"
        ")\n"
        "WITH (\n"
        f"{corpo_propriedades}\n"
        ")"
    )
