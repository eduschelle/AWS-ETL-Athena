"""
Job SILVER: bronze cru -> indicadores fundamentalistas tipados + serie historica real.

Grava DUAS tabelas a partir da mesma leitura:
  - `silver_indicadores_fundamentalistas`: os 22 indicadores do Fundamentus, derivados e
    metadados de procedencia, um registro por (ticker, data_coleta).
  - `silver_precos_brapi`: a serie de ~63 pregoes REAIS que vem na mesma resposta da API.

Este job NAO toca em `precos_acoes`. Aquela tabela e um random walk sintetico do pipeline de
streaming; derivar volatilidade ou liquidez dela e alimentar um score de investimento
produziria um numero que parece real e e ruido puro. A serie real vem da brapi, a custo
marginal zero de quota (mesma requisicao dos fundamentos).

Toda a matematica vive em `brapi_mapeamento`, testada isoladamente. Aqui so ha I/O.
"""

import json
import logging

import brapi_mapeamento
import spark_comum

TABELA_ORIGEM = "bronze_fundamentos"
TABELA_INDICADORES = "silver_indicadores_fundamentalistas"
TABELA_PRECOS = "silver_precos_brapi"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("job_silver_indicadores")


def transformar(envelopes: list[dict], data_coleta: str = "") -> tuple[list[dict], list[dict]]:
    """Converte os envelopes do bronze nas linhas das duas tabelas silver."""
    indicadores = []
    precos = []

    for envelope in envelopes:
        ticker = envelope.get("ticker")
        bruto = envelope.get("payload_json")

        if not bruto:
            # Coleta que falhou (tipicamente HTTP 401 por falta de token). Registramos a
            # linha de todo modo: um ticker ausente do silver seria indistinguivel de um
            # ticker que nunca foi pedido.
            log.warning(
                "%s: sem payload (http_status=%s). Linha marcada como inelegivel.",
                ticker,
                envelope.get("http_status"),
            )
            payload = {}
        else:
            try:
                payload = json.loads(bruto)
            except json.JSONDecodeError:
                log.error("%s: payload_json invalido, tratado como vazio.", ticker)
                payload = {}

        linha = brapi_mapeamento.extrair_indicadores(
            payload,
            ticker=ticker,
            fonte=envelope.get("fonte") or "brapi",
            coletado_em=envelope.get("coletado_em"),
            data_coleta=envelope.get("data_coleta") or data_coleta,
        )
        indicadores.append(linha)

        precos.extend(
            brapi_mapeamento.linhas_de_serie(
                ticker,
                payload.get("historicalDataPrice"),
                data_coleta=envelope.get("data_coleta") or data_coleta,
            )
        )

    return indicadores, precos


def main() -> None:
    data_coleta = spark_comum.resolver_data_coleta()
    spark = spark_comum.criar_sessao("b3-silver-indicadores")

    try:
        bronze = spark_comum.ler_particao(
            spark, TABELA_ORIGEM, "data_coleta", data_coleta
        )
        envelopes = [linha.asDict() for linha in bronze.collect()]

        if not envelopes:
            log.warning(
                "Nenhum registro no bronze para data_coleta=%s. Encerrando.", data_coleta
            )
            return

        log.info("%s envelopes lidos do bronze.", len(envelopes))
        indicadores, precos = transformar(envelopes, data_coleta)

        elegiveis = sum(1 for linha in indicadores if linha["elegivel_score"])
        log.info(
            "%s indicadores (%s elegiveis para score), %s pregoes de serie.",
            len(indicadores),
            elegiveis,
            len(precos),
        )

        spark_comum.gravar_tabela(spark, indicadores, TABELA_INDICADORES)
        if precos:
            spark_comum.gravar_tabela(spark, precos, TABELA_PRECOS)
        else:
            log.warning("Nenhuma serie historica no payload; %s nao foi atualizada.", TABELA_PRECOS)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
