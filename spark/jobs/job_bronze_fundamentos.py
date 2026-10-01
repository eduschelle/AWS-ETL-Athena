"""
Job BRONZE: fila do Redis (ou landing, em replay) -> Parquet cru no Data Lake.

A camada bronze guarda o JSON da resposta HTTP intacto, numa unica coluna VARCHAR. Isso e
deliberado: a brapi devolve `null` em metade dos campos de forma imprevisivel por tipo de
ativo (banco != BDR != ETF), e tipar nesta camada transformaria qualquer mudanca na API num
job quebrado. Schema-on-read aqui, tipagem no silver.

Bronze tambem e o que torna silver e gold reconstruiveis sem gastar uma unica requisicao de
quota — motivo pelo qual ela nao e uma camada redundante neste projeto.

ACK EXPLICITO DA FILA: o drain usa RPOPLPUSH para uma lista de processamento e so apaga essa
lista depois da gravacao bem-sucedida. O `spark_job.py` original usa RPOP direto, o que
significa que uma falha apos o drain perde os eventos. Para precos sinteticos isso e
irrelevante (basta rodar o producer de novo); para dados que custaram requisicoes de uma quota
mensal, nao e. De quebra, isto corrige a corrida entre DAG runs concorrentes documentada no
SKILL.md: sobras de uma execucao interrompida sao recuperadas na execucao seguinte.
"""

import json
import logging
import os

import spark_comum

FILA = os.environ.get("REDIS_QUEUE_FUNDAMENTOS", "b3:fundamentos")
FILA_PROCESSANDO = f"{FILA}:processando"
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

TABELA = "bronze_fundamentos"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("job_bronze_fundamentos")


def _cliente_redis():
    import redis

    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def drenar_fila(cliente) -> list[dict]:
    """Move tudo da fila para a lista de processamento e le sem destruir.

    Comeca movendo o que ainda estiver na fila principal; sobras ja presentes na lista de
    processamento (execucao anterior que falhou antes do ack) sao incluidas automaticamente,
    porque a leitura e feita sobre ela.
    """
    while cliente.rpoplpush(FILA, FILA_PROCESSANDO) is not None:
        pass

    brutos = cliente.lrange(FILA_PROCESSANDO, 0, -1)
    envelopes = []
    for bruto in brutos:
        try:
            envelopes.append(json.loads(bruto))
        except json.JSONDecodeError:
            log.error("Item invalido na fila, ignorado: %.120s", bruto)
    return envelopes


def deduplicar(envelopes: list[dict]) -> list[dict]:
    """Mantem apenas a coleta mais recente de cada (ticker, data_coleta)."""
    melhores: dict[tuple, dict] = {}
    for envelope in envelopes:
        chave = (envelope.get("ticker"), envelope.get("data_coleta"))
        atual = melhores.get(chave)
        if atual is None or (envelope.get("coletado_em") or "") > (
            atual.get("coletado_em") or ""
        ):
            melhores[chave] = envelope
    return list(melhores.values())


def main() -> None:
    fonte_entrada = spark_comum.resolver_fonte_entrada()
    data_coleta = spark_comum.resolver_data_coleta()
    cliente = None

    if fonte_entrada == "landing":
        import coletor_brapi

        log.info("Modo replay: lendo a landing de data_coleta=%s (sem HTTP).", data_coleta)
        envelopes = coletor_brapi.ler_landing(data_coleta)
    elif fonte_entrada == "redis":
        cliente = _cliente_redis()
        envelopes = drenar_fila(cliente)
    else:
        raise ValueError(f"fonte-entrada invalida: {fonte_entrada!r}. Use redis ou landing.")

    if not envelopes:
        log.warning("Nenhum envelope para processar. Encerrando sem gravar.")
        return

    envelopes = deduplicar(envelopes)
    log.info("%s envelopes apos deduplicacao.", len(envelopes))

    spark = spark_comum.criar_sessao("b3-bronze-fundamentos")
    try:
        gravadas = spark_comum.gravar_tabela(spark, envelopes, TABELA)
    finally:
        spark.stop()

    # ACK: so agora os itens saem da fila em definitivo.
    if cliente is not None and gravadas:
        cliente.delete(FILA_PROCESSANDO)
        log.info("Fila de processamento liberada (%s itens confirmados).", gravadas)


if __name__ == "__main__":
    main()
