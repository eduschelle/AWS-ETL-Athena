"""
Integracao do pipeline inteiro sem Spark: coleta -> bronze -> silver -> gold.

Este e o teste que de fato valida o desenho. Ele monta o universo real de 14 tickers no modo
degradado (sem token), atravessa as mesmas funcoes que os jobs Spark chamam e verifica o
resultado final — tudo em memoria, em milissegundos, sem Docker e sem rede.
"""

import json

import pytest

import brapi_mapeamento as bm
import categorias
import coletor_brapi as cb
import esquemas
import job_gold_classificacao as gold
import job_silver_indicadores as silver
import pontuacao

DATA = "2026-10-01"


@pytest.fixture(scope="module")
def envelopes_bronze(payload_petr4_modulo) -> list[dict]:
    """Simula a coleta do modo degradado: 4 tickers reais, 10 sinteticos.

    PETR4 usa o payload real capturado da API; os outros 3 tickers livres nao podem ser
    buscados num teste offline, entao entram como falha de coleta (payload nulo), o que
    tambem exercita o caminho de erro.
    """
    universo = cb.resolver_universo(modo="auto", token="")
    envelopes = []

    for ticker, fonte in universo:
        if fonte == "sintetico":
            payload, status = cb.gerar_sintetico(ticker, DATA), 0
        elif ticker == "PETR4":
            payload, status = payload_petr4_modulo, 200
        else:
            payload, status = None, 401  # sem rede no teste

        envelopes.append(cb.montar_envelope(ticker, payload, fonte, status, DATA))

    return envelopes


@pytest.fixture(scope="module")
def payload_petr4_modulo() -> dict:
    import io
    from pathlib import Path

    caminho = Path(__file__).parent / "fixtures" / "brapi_petr4.json"
    return json.loads(io.open(caminho, encoding="utf-8").read())


@pytest.fixture(scope="module")
def camada_silver(envelopes_bronze):
    return silver.transformar(envelopes_bronze)


@pytest.fixture(scope="module")
def camada_gold(camada_silver):
    indicadores, _ = camada_silver
    return gold.classificar([dict(linha) for linha in indicadores], DATA)


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
def test_bronze_cobre_todo_o_universo(envelopes_bronze):
    assert len(envelopes_bronze) == len(cb.TICKERS_PADRAO)
    for envelope in envelopes_bronze:
        assert set(envelope) == set(esquemas.colunas("bronze_fundamentos"))


def test_bronze_deduplica_recoleta():
    """Reprocessar a mesma particao nao pode duplicar o ticker."""
    import job_bronze_fundamentos as bronze

    antigo = cb.montar_envelope("PETR4", {"symbol": "PETR4"}, "brapi", 200, DATA)
    antigo["coletado_em"] = "2026-10-01T08:00:00+00:00"
    novo = cb.montar_envelope("PETR4", {"symbol": "PETR4"}, "brapi", 200, DATA)
    novo["coletado_em"] = "2026-10-01T18:00:00+00:00"

    resultado = bronze.deduplicar([antigo, novo])
    assert len(resultado) == 1
    assert resultado[0]["coletado_em"] == novo["coletado_em"]


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
def test_silver_mantem_uma_linha_por_ticker(camada_silver):
    indicadores, _ = camada_silver
    tickers = [linha["ticker"] for linha in indicadores]
    assert len(tickers) == len(set(tickers)) == len(cb.TICKERS_PADRAO)


def test_silver_respeita_o_schema(camada_silver):
    indicadores, precos = camada_silver
    esperado_ind = set(esquemas.colunas("silver_indicadores_fundamentalistas"))
    for linha in indicadores:
        assert set(linha) == esperado_ind

    esperado_precos = set(esquemas.colunas("silver_precos_brapi"))
    for linha in precos:
        assert set(linha) == esperado_precos


def test_silver_registra_coleta_falha_sem_descartar_o_ticker(camada_silver):
    """Um ticker ausente do silver seria indistinguivel de um que nunca foi pedido."""
    indicadores, _ = camada_silver
    por_ticker = {linha["ticker"]: linha for linha in indicadores}

    # VALE3/ITUB4/MGLU3 falharam a coleta neste cenario offline.
    falhou = por_ticker["VALE3"]
    assert falhou["elegivel_score"] is False
    assert falhou["cotacao"] is None
    assert falhou["motivo_inelegibilidade"] == "sem_cotacao"


def test_silver_nao_tem_nan_em_lugar_nenhum(camada_silver):
    """NaN em DOUBLE nao e NULL no Spark: passaria por isNotNull() e envenenaria medias."""
    import math

    indicadores, precos = camada_silver
    for linha in indicadores + precos:
        for chave, valor in linha.items():
            if isinstance(valor, float):
                assert not math.isnan(valor), f"{chave} e NaN"
                assert not math.isinf(valor), f"{chave} e infinito"


def test_silver_produz_serie_historica_real(camada_silver):
    _, precos = camada_silver
    assert precos
    petr4 = [linha for linha in precos if linha["ticker"] == "PETR4"]
    assert len(petr4) == 63
    datas = {linha["data_pregao"] for linha in petr4}
    assert len(datas) == 63  # uma particao por pregao, sem duplicatas


def test_silver_marca_financeiras(camada_silver):
    """ITUB4 fica de fora: e um dos 4 tickers livres, logo falha a coleta neste teste offline
    e chega sem setor. Os demais bancos sao sinteticos e trazem o setor preenchido."""
    indicadores, _ = camada_silver
    por_ticker = {linha["ticker"]: linha for linha in indicadores}
    for banco in ("BBDC4", "BBAS3", "ITSA4", "B3SA3"):
        assert por_ticker[banco]["setor_financeiro"] is True, banco
    assert por_ticker["PETR4"]["setor_financeiro"] is False


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
def test_gold_respeita_o_schema(camada_gold):
    esperado = set(esquemas.colunas("gold_classificacao_ativos"))
    for linha in camada_gold:
        assert esperado <= set(linha), f"faltando: {esperado - set(linha)}"


def test_gold_tem_uma_linha_por_ticker(camada_gold):
    tickers = [linha["ticker"] for linha in camada_gold]
    assert len(tickers) == len(set(tickers)) == len(cb.TICKERS_PADRAO)


def test_gold_nunca_mistura_fontes_no_ranking(camada_gold):
    """A garantia central do modo degradado: dado real nao e rankeado contra sintetico."""
    por_ticker = {linha["ticker"]: linha for linha in camada_gold}
    petr4 = por_ticker["PETR4"]
    assert petr4["fonte"] == "brapi"
    # PETR4 e o unico ativo real elegivel neste cenario -> grupo de tamanho 1.
    assert petr4["n_grupo"] == 1
    assert petr4["grupo_usado"] == "universo"

    sintetico = por_ticker["WEGE3"]
    assert sintetico["fonte"] == "sintetico"
    assert sintetico["n_grupo"] > 1


def test_gold_grupo_pequeno_usa_apenas_banda_absoluta(camada_gold):
    """Com n=1 o percentil nao entra; o score e a banda pura — e isso fica visivel no dado."""
    petr4 = next(linha for linha in camada_gold if linha["ticker"] == "PETR4")
    assert petr4["n_grupo"] < pontuacao.MIN_GRUPO
    assert petr4["score_final"] is not None
    assert 0.0 <= petr4["score_final"] <= 100.0


def test_gold_petr4_recebe_score_coerente(camada_gold):
    """Petrobras com dados reais: P/L ~4.7, ROE ~28%, DY 7% — tem que pontuar bem."""
    petr4 = next(linha for linha in camada_gold if linha["ticker"] == "PETR4")
    assert petr4["cobertura_indicadores_pct"] == pytest.approx(1.0)
    assert petr4["confianca"] == "alta"
    assert petr4["score_valuation"] > 70  # multiplo muito baixo
    assert petr4["score_rentabilidade"] > 60  # ROE 28%, ROIC 11%
    assert petr4["score_dividendos"] > 70  # DY 7%
    assert petr4["nota"] in ("A", "B", "C")
    # Endividamento alto (div.liq/PL ~1.29) deve segurar o pilar de solidez.
    assert petr4["score_solidez"] < 60


def test_gold_classifica_categorias_do_petr4(camada_gold):
    petr4 = next(linha for linha in camada_gold if linha["ticker"] == "PETR4")
    assert petr4["classe_ativo"] == "ACAO_PN"
    assert petr4["porte"] == "LARGE"
    assert petr4["setor"] == "Energia"
    assert petr4["perfil_risco"] in ("BAIXO", "MEDIO", "ALTO")
    assert petr4["perfil_investidor"] == "DIVIDENDOS"  # DY 7% com risco nao-alto


def test_gold_inelegiveis_ficam_sem_score(camada_gold):
    por_ticker = {linha["ticker"]: linha for linha in camada_gold}
    # BDR e coletas que falharam.
    for ticker in ("MUTC34", "VALE3", "ITUB4", "MGLU3"):
        assert por_ticker[ticker]["score_final"] is None, ticker
        assert por_ticker[ticker]["nota"] == "SD", ticker


def test_gold_notas_cobrem_mais_de_uma_faixa(camada_gold):
    """Se todo o universo recebesse a mesma nota, o score nao estaria discriminando."""
    notas = {linha["nota"] for linha in camada_gold if linha["nota"] != "SD"}
    assert len(notas) >= 2


def test_gold_scores_dentro_da_faixa(camada_gold):
    campos = (
        "score_final",
        "score_valuation",
        "score_rentabilidade",
        "score_solidez",
        "score_dividendos",
    )
    for linha in camada_gold:
        for campo in campos:
            valor = linha[campo]
            if valor is not None:
                assert 0.0 <= valor <= 100.0, f"{linha['ticker']}.{campo} = {valor}"


def test_gold_nota_corresponde_ao_score(camada_gold):
    for linha in camada_gold:
        assert linha["nota"] == pontuacao.nota_letra(linha["score_final"])


def test_gold_bancos_usam_perfil_financeiro(camada_gold):
    por_ticker = {linha["ticker"]: linha for linha in camada_gold}
    banco = por_ticker["BBDC4"]
    assert banco["perfil_pesos"] == "financeiro"
    # Sem o perfil financeiro, liq_corrente e divida/EBITDA nulos zerariam este pilar.
    assert banco["score_solidez"] is not None


def test_gold_magic_formula_ranqueia_dentro_da_fonte(camada_gold):
    ranks = [
        linha["rank_magic_formula"]
        for linha in camada_gold
        if linha["fonte"] == "sintetico" and linha["rank_magic_formula"] is not None
    ]
    assert len(ranks) >= 2
    assert min(ranks) >= 2  # melhor caso possivel: rank 1 + rank 1


def test_gold_particao_e_a_data_de_referencia(camada_gold):
    assert all(linha["data_referencia"] == DATA for linha in camada_gold)


# ---------------------------------------------------------------------------
# Idempotencia
# ---------------------------------------------------------------------------
def test_pipeline_e_deterministico(envelopes_bronze):
    """Reprocessar a mesma particao tem que produzir exatamente o mesmo resultado.

    E o que permite usar overwrite dinamico da particao em vez de append, e portanto o que
    torna a DAG re-executavel sem duplicar nem alterar numeros.
    """
    primeira = gold.classificar(
        [dict(linha) for linha in silver.transformar(envelopes_bronze)[0]], DATA
    )
    segunda = gold.classificar(
        [dict(linha) for linha in silver.transformar(envelopes_bronze)[0]], DATA
    )

    chave = lambda linhas: sorted(  # noqa: E731
        (linha["ticker"], linha["nota"], linha["score_final"]) for linha in linhas
    )
    assert chave(primeira) == chave(segunda)


def test_categorias_rodam_depois_do_score(camada_gold):
    """`perfil_investidor` depende de score_valuation/score_solidez — a ordem importa."""
    com_valor = [
        linha
        for linha in camada_gold
        if linha["perfil_investidor"] == "VALOR"
        and linha["score_valuation"] is not None
    ]
    for linha in com_valor:
        assert linha["score_valuation"] >= categorias.SCORE_MINIMO_VALOR_VALUATION
        assert linha["score_solidez"] >= categorias.SCORE_MINIMO_VALOR_SOLIDEZ
