"""
Registro de tabelas no catalogo (Hive Metastore via Trino) — o equivalente a um crawler do
AWS Glue.

Generaliza o que antes era a funcao `atualizar_catalogo()` embutida em `pipeline_dag.py`, que
tinha o DDL de `precos_acoes` escrito a mao. Agora o DDL vem de `esquemas.ddl_trino()`, de
forma que schema do Spark e schema do Trino nao possam mais divergir.

Depende de `PYTHONPATH=/opt/airflow/spark_jobs` (definido no docker-compose): por padrao o
Airflow so coloca /opt/airflow/dags e /opt/airflow/plugins no sys.path.
"""

import logging
import os
from urllib.parse import urlparse

import esquemas

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USUARIO = os.environ.get("TRINO_USER", "airflow")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

log = logging.getLogger(__name__)

NOME_VIEW_SCREENER = "gold_screener_fundamentus"

# Projecao das 22 colunas do screener do Fundamentus, na ordem e com os rotulos do site.
# Identificadores entre aspas duplas no Trino permitem reproduzir nomes como "P/L" e
# "Dív.Líq/Patrim." literalmente, o que torna a view imediatamente reconhecivel.
COLUNAS_SCREENER = [
    ('i.ticker', "Papel"),
    ('i.cotacao', "Cotação"),
    ('i.preco_lucro', "P/L"),
    ('i.preco_valor_patrimonial', "P/VP"),
    ('i.psr', "PSR"),
    ('i.dividend_yield', "Div.Yield"),
    ('i.preco_ativo', "P/Ativo"),
    ('i.preco_capital_giro', "P/Cap.Giro"),
    ('i.preco_ebit', "P/EBIT"),
    ('i.preco_ativo_circ_liquido', "P/Ativ Circ.Liq"),
    ('i.ev_ebit', "EV/EBIT"),
    ('i.ev_ebitda', "EV/EBITDA"),
    ('i.margem_bruta', "Mrg Bruta"),
    ('i.margem_ebit', "Mrg Ebit"),
    ('i.margem_liquida', "Mrg. Líq."),
    ('i.liq_corrente', "Liq. Corr."),
    ('i.roic', "ROIC"),
    ('i.roe', "ROE"),
    ('i.liquidez_2meses', "Liq.2meses"),
    ('i.patrimonio_liquido', "Patrim. Líq"),
    ('i.divida_liquida_patrimonio', "Dív.Líq/Patrim."),
    ('i.cresc_receita_5a', "Cresc. Rec.5a"),
    # Acrescimos deste projeto: a classificacao, que o Fundamentus nao tem.
    ('g.nota', "Nota"),
    ('g.score_final', "Score"),
    ('g.setor', "Setor"),
    ('g.porte', "Porte"),
    ('g.perfil_risco', "Risco"),
    ('g.perfil_investidor', "Perfil"),
    ('g.confianca', "Confiança"),
    ('i.fonte', "Fonte"),
]


def conectar():
    import trino

    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USUARIO,
        catalog=esquemas.CATALOGO,
        schema=esquemas.SCHEMA,
    )


def _executar(cursor, sql: str) -> None:
    log.info("Trino <- %s", sql.strip().splitlines()[0][:120])
    cursor.execute(sql)
    cursor.fetchall()


def _garantir_prefixo(localizacao_s3: str) -> None:
    """Cria o prefixo no MinIO se ele nao existir.

    `CREATE TABLE ... WITH (external_location = ...)` falha quando o caminho nao existe. Esse
    e um bug latente no pipeline original: na primeira execucao com a fila vazia, o Spark nao
    escreve nada e a task de catalogo quebra. Criar o prefixo antes elimina a dependencia da
    ordem entre escrita e registro.
    """
    import boto3

    partes = urlparse(localizacao_s3)
    bucket = partes.netloc
    prefixo = partes.path.lstrip("/")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    existente = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo, MaxKeys=1)
    if existente.get("KeyCount", 0) > 0:
        return

    log.info("Criando prefixo vazio s3://%s/%s", bucket, prefixo)
    s3.put_object(Bucket=bucket, Key=prefixo, Body=b"")


def registrar_tabela(nome: str, **_) -> None:
    """Garante que a tabela exista no catalogo e sincroniza as particoes gravadas pelo Spark."""
    _garantir_prefixo(esquemas.localizacao(nome))

    conexao = conectar()
    cursor = conexao.cursor()

    _executar(cursor, esquemas.ddl_trino(nome))

    particoes = esquemas.colunas_particao(nome)
    if not particoes:
        # `sync_partition_metadata` falha com "Table is not partitioned" se chamada numa
        # tabela sem particao — e nao se aplica a views.
        log.info("%s nao e particionada; nada a sincronizar.", nome)
        return

    _executar(
        cursor,
        f"""
        CALL {esquemas.CATALOGO}.system.sync_partition_metadata(
            schema_name => '{esquemas.SCHEMA}',
            table_name => '{nome}',
            mode => 'FULL'
        )
        """,
    )
    log.info("Tabela %s registrada e particoes sincronizadas.", nome)


def sql_view_screener() -> str:
    """Monta o CREATE OR REPLACE VIEW do screener no formato do Fundamentus.

    E uma VIEW, nao uma tabela: o conteudo e projecao e renomeacao puras sobre silver e gold.
    Materializar duplicaria os dados e exigiria um quarto job Spark sem nenhum ganho.
    """
    projecao = ",\n".join(
        f'    {origem} AS "{rotulo}"' for origem, rotulo in COLUNAS_SCREENER
    )
    indicadores = esquemas.nome_completo("silver_indicadores_fundamentalistas")
    classificacao = esquemas.nome_completo("gold_classificacao_ativos")

    return f"""
CREATE OR REPLACE VIEW {esquemas.nome_completo(NOME_VIEW_SCREENER)} AS
SELECT
{projecao}
FROM {indicadores} AS i
LEFT JOIN {classificacao} AS g
       ON g.ticker = i.ticker
      AND g.data_referencia = i.data_coleta
WHERE i.data_coleta = (SELECT max(data_coleta) FROM {indicadores})
"""


def criar_view_screener(**_) -> None:
    conexao = conectar()
    cursor = conexao.cursor()
    _executar(cursor, sql_view_screener())
    log.info("View %s atualizada.", NOME_VIEW_SCREENER)
