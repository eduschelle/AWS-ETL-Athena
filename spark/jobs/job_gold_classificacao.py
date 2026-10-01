"""
Job GOLD: indicadores do silver -> score 0-100, nota A-E e classificacao por categoria.

Esta e a tabela que responde a pergunta do projeto: "quais ativos estao bem classificados, e
por que". Cada numero e rastreavel — `grupo_usado` diz contra quem o ativo foi comparado,
`cobertura_indicadores_pct` diz quanto do score pode ser calculado, e os `*_origem` do silver
dizem de onde cada indicador veio.

Particionada por `data_referencia` e gravada com overwrite DINAMICO: reexecutar o mesmo dia
substitui a particao (idempotencia) sem apagar o historico das semanas anteriores. Esse
historico custa ~14 linhas por execucao e habilita acompanhar a evolucao do score com LAG().

O calculo roda em Python puro no driver, sobre a lista de linhas. Com algumas dezenas de
ativos isso e mais simples, mais rapido e infinitamente mais testavel que montar Window
functions — e a logica de grupo de comparacao em cascata e trivial em Python e horrivel em SQL
de janela. Acima de ~10^5 ativos valeria migrar o percentil para `Window.percent_rank()`.
"""

import logging

import categorias
import pontuacao
import spark_comum

TABELA_ORIGEM = "silver_indicadores_fundamentalistas"
TABELA_DESTINO = "gold_classificacao_ativos"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("job_gold_classificacao")


def classificar(linhas: list[dict], data_referencia: str) -> list[dict]:
    """Pontua e categoriza o universo, devolvendo as linhas da tabela gold."""
    pontuadas = pontuacao.pontuar(linhas)

    resultado = []
    for linha in pontuadas:
        # As categorias rodam DEPOIS do score porque `perfil_investidor` depende das notas
        # de valuation e solidez.
        linha.update(categorias.classificar(linha))
        linha["data_referencia"] = data_referencia
        resultado.append(linha)

    return resultado


def main() -> None:
    data_coleta = spark_comum.resolver_data_coleta()
    spark = spark_comum.criar_sessao("b3-gold-classificacao")

    try:
        silver = spark_comum.ler_particao(
            spark, TABELA_ORIGEM, "data_coleta", data_coleta
        )
        linhas = [registro.asDict() for registro in silver.collect()]

        if not linhas:
            log.warning(
                "Nenhum indicador no silver para data_coleta=%s. Encerrando.", data_coleta
            )
            return

        log.info("%s ativos lidos do silver.", len(linhas))
        classificadas = classificar(linhas, data_coleta)

        # Resumo no log: e o primeiro lugar onde se percebe que algo saiu errado, antes de
        # qualquer consulta no Trino.
        distribuicao: dict[str, int] = {}
        for linha in classificadas:
            distribuicao[linha["nota"]] = distribuicao.get(linha["nota"], 0) + 1
        log.info("Distribuicao de notas: %s", dict(sorted(distribuicao.items())))

        for linha in sorted(
            classificadas,
            key=lambda item: (item["score_final"] is None, -(item["score_final"] or 0)),
        ):
            log.info(
                "  %-8s nota=%-3s score=%-6s grupo=%-9s n=%-3s cobertura=%.0f%% fonte=%s",
                linha["ticker"],
                linha["nota"],
                "n/d"
                if linha["score_final"] is None
                else f"{linha['score_final']:.1f}",
                linha["grupo_usado"] or "-",
                linha["n_grupo"],
                (linha["cobertura_indicadores_pct"] or 0) * 100,
                linha["fonte"],
            )

        spark_comum.gravar_tabela(spark, classificadas, TABELA_DESTINO)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
