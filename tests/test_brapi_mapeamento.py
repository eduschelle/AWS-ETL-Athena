"""Testes do mapeamento brapi -> indicadores do Fundamentus."""

import math

import pytest
from conftest import serie_sintetica

import brapi_mapeamento as bm
import esquemas


# ---------------------------------------------------------------------------
# Helpers numericos
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "entrada",
    [None, "abc", float("nan"), float("inf"), float("-inf"), True, False, {}, []],
)
def test_num_rejeita_valores_nao_numericos(entrada):
    assert bm.num(entrada) is None


def test_num_aceita_numeros_e_strings_numericas():
    assert bm.num(3) == 3.0
    assert bm.num("2.5") == 2.5


@pytest.mark.parametrize(
    "a,b",
    [(1, 0), (1, None), (None, 1), (None, None), (1, float("nan"))],
)
def test_div_segura_nunca_levanta(a, b):
    assert bm.div_segura(a, b) is None


def test_div_segura_calcula():
    assert bm.div_segura(10, 4) == 2.5


def test_aplicar_sanidade_descarta_fora_da_faixa():
    # DY de 350% nao e um extremo legitimo, e erro de dado.
    assert bm.aplicar_sanidade("dividend_yield", 3.5) is None
    assert bm.aplicar_sanidade("dividend_yield", 0.07) == 0.07
    # Indicador sem faixa cadastrada passa direto.
    assert bm.aplicar_sanidade("campo_sem_limite", 999_999) == 999_999


def test_limpar_nao_finito_troca_nan_por_none():
    limpa = bm.limpar_nao_finito({"a": float("nan"), "b": float("inf"), "c": 1.0, "d": "x"})
    assert limpa == {"a": None, "b": None, "c": 1.0, "d": "x"}


# ---------------------------------------------------------------------------
# Payload real do PETR4
# ---------------------------------------------------------------------------
def test_mapeamento_produz_exatamente_as_colunas_do_silver(payload_petr4):
    linha = bm.extrair_indicadores(payload_petr4, data_coleta="2026-10-01")
    assert set(linha) == set(esquemas.colunas("silver_indicadores_fundamentalistas"))


def test_patrimonio_liquido_derivado_bate_com_debt_to_equity(payload_petr4):
    """A validacao cruzada que justifica a derivacao bookValue * sharesOutstanding.

    `totalStockholderEquity` nao vem no plano free, entao o patrimonio e derivado. Se a
    derivacao estiver certa, totalDebt / patrimonio tem que reproduzir o `debtToEquity` que
    a propria API informa — e reproduz, com erro de arredondamento desprezivel.
    """
    linha = bm.extrair_indicadores(payload_petr4)
    debt_to_equity_api = payload_petr4["financialData"]["debtToEquity"]
    calculado = linha["divida_total"] / linha["patrimonio_liquido"]
    assert calculado == pytest.approx(debt_to_equity_api, rel=1e-4)


def test_razoes_sao_recalculadas_a_partir_da_cotacao(payload_petr4):
    """Campos pre-calculados da brapi estao defasados; os nossos tem que ser coerentes."""
    linha = bm.extrair_indicadores(payload_petr4)
    cotacao = payload_petr4["regularMarketPrice"]

    assert linha["preco_lucro"] == pytest.approx(cotacao / linha["lpa"])
    assert linha["preco_valor_patrimonial"] == pytest.approx(cotacao / linha["vpa"])
    assert linha["valor_mercado"] == pytest.approx(cotacao * linha["acoes_emitidas"])

    # Conferencia contra o campo pronto que E coerente com a cotacao.
    assert linha["preco_valor_patrimonial"] == pytest.approx(
        payload_petr4["defaultKeyStatistics"]["priceToBook"], rel=1e-4
    )


def test_debt_to_equity_e_razao_nao_percentual(payload_petr4):
    """Convencao do Yahoo seria percentual (140.6); a brapi devolve razao (1.406).

    Se alguem "corrigir" isso dividindo por 100, o endividamento de todas as empresas cairia
    por um fator de 100 e o pilar de solidez daria nota maxima para todo mundo.
    """
    linha = bm.extrair_indicadores(payload_petr4)
    assert 0.1 < linha["divida_bruta_patrimonio"] < 10
    # Divida liquida e menor que a bruta, porque desconta o caixa.
    assert linha["divida_liquida_patrimonio"] < linha["divida_bruta_patrimonio"]


def test_indicadores_indisponiveis_no_plano_free_ficam_nulos(payload_petr4):
    linha = bm.extrair_indicadores(payload_petr4)
    assert linha["preco_capital_giro"] is None
    assert linha["preco_ativo_circ_liquido"] is None
    assert linha["cresc_receita_5a"] is None
    # Mas o proxy de 12 meses existe e fica carimbado.
    assert linha["cresc_receita_12m"] is not None
    assert linha["cresc_receita_origem"] == "proxy_12m"


def test_preco_ativo_usa_proxy_por_roa(payload_petr4):
    """`totalAssets` vem nulo; ativos = lucro / ROA. Ordem de grandeza tem que fechar."""
    assert payload_petr4["defaultKeyStatistics"]["totalAssets"] is None
    linha = bm.extrair_indicadores(payload_petr4)
    assert linha["preco_ativo_origem"] == "estimado_por_roa"
    # Petrobras: ativo total na casa do trilhao de reais.
    assert 5e11 < linha["ativo_total_estimado"] < 5e12


def test_petr4_e_elegivel_com_cobertura_total(payload_petr4):
    linha = bm.extrair_indicadores(payload_petr4)
    assert linha["elegivel_score"] is True
    assert linha["motivo_inelegibilidade"] is None
    assert linha["cobertura_campos_pct"] == 1.0
    assert linha["classe_ativo"] == "ACAO_PN"
    assert linha["setor_financeiro"] is False


def test_metricas_de_serie_do_payload_real(payload_petr4):
    linha = bm.extrair_indicadores(payload_petr4)
    assert linha["pontos_serie"] == 63
    assert linha["liquidez_2meses_origem"] == "serie_brapi_3mo"
    # Liquidez de uma blue chip: centenas de milhoes a bilhoes de reais por pregao.
    assert linha["liquidez_2meses"] > 1e8
    # Volatilidade anualizada plausivel para acao brasileira: entre 10% e 100%.
    assert 0.10 < linha["volatilidade_anualizada"] < 1.0


# ---------------------------------------------------------------------------
# Casos de borda por tipo de ativo
# ---------------------------------------------------------------------------
def test_banco_atravessa_sem_excecao_e_com_nulos_corretos(payload_banco):
    linha = bm.extrair_indicadores(payload_banco, data_coleta="2026-10-01")

    # Metricas estruturalmente inaplicaveis a bancos.
    assert linha["margem_bruta"] is None
    assert linha["liq_corrente"] is None
    assert linha["margem_ebit"] is None
    assert linha["ebitda"] is None
    assert linha["divida_liquida_ebitda"] is None
    # EBIT nao reconstruivel sem operatingMargins -> os derivados dele somem.
    assert linha["ebit_estimado"] is None
    assert linha["preco_ebit"] is None
    assert linha["ev_ebit"] is None
    assert linha["roic"] is None
    assert linha["roic_origem"] == "indisponivel"

    # Mas o que existe para banco tem que estar la.
    assert linha["setor_financeiro"] is True
    assert linha["roe"] == pytest.approx(0.185)
    assert linha["preco_lucro"] == pytest.approx(33.80 / 3.80)
    assert linha["elegivel_score"] is True


def test_bdr_vazio_fica_inelegivel(payload_bdr_vazio):
    linha = bm.extrair_indicadores(payload_bdr_vazio)
    assert linha["classe_ativo"] == "BDR"
    assert linha["elegivel_score"] is False
    assert linha["motivo_inelegibilidade"] == "classe_nao_suportada"


def test_sem_cotacao_fica_inelegivel():
    linha = bm.extrair_indicadores({"symbol": "WEGE3"})
    assert linha["cotacao"] is None
    assert linha["elegivel_score"] is False
    assert linha["motivo_inelegibilidade"] == "sem_cotacao"


def test_acao_com_poucos_dados_fica_inelegivel():
    linha = bm.extrair_indicadores(
        {"symbol": "XPTO3", "regularMarketPrice": 10.0, "financialData": {}}
    )
    assert linha["classe_ativo"] == "ACAO_ON"
    assert linha["elegivel_score"] is False
    assert linha["motivo_inelegibilidade"] == "dados_insuficientes"


def test_receita_zero_nao_divide_por_zero():
    linha = bm.extrair_indicadores(
        {
            "symbol": "XPTO3",
            "regularMarketPrice": 10.0,
            "defaultKeyStatistics": {"sharesOutstanding": 1_000_000, "bookValue": 5.0},
            "financialData": {"totalRevenue": 0, "operatingMargins": 0.2},
        }
    )
    assert linha["psr"] is None
    assert linha["ebit_estimado"] == 0.0
    # EBIT zero no denominador tambem nao pode explodir.
    assert linha["preco_ebit"] is None


def test_prejuizo_marca_lucro_negativo():
    linha = bm.extrair_indicadores(
        {"symbol": "XPTO3", "regularMarketPrice": 10.0, "earningsPerShare": -2.0}
    )
    assert linha["lucro_negativo"] is True
    assert linha["preco_lucro"] == pytest.approx(-5.0)


def test_margem_ebit_nunca_cai_para_ebitda():
    """Confundir os dois inflaria a margem operacional de empresas intensivas em capital."""
    linha = bm.extrair_indicadores(
        {
            "symbol": "XPTO3",
            "regularMarketPrice": 10.0,
            "financialData": {
                "totalRevenue": 1000,
                "operatingMargins": None,
                "ebitdaMargins": 0.45,
            },
        }
    )
    assert linha["margem_ebit"] is None
    assert linha["margem_ebitda"] == pytest.approx(0.45)


# ---------------------------------------------------------------------------
# Serie historica
# ---------------------------------------------------------------------------
def test_serie_curta_nao_produz_metricas():
    metricas = bm.metricas_de_serie(serie_sintetica(bm.MIN_PONTOS_SERIE - 1))
    assert metricas["liquidez_2meses"] is None
    assert metricas["volatilidade_anualizada"] is None
    assert metricas["liquidez_2meses_origem"] == "amostra_insuficiente"


def test_serie_suficiente_produz_metricas():
    metricas = bm.metricas_de_serie(serie_sintetica(60, preco_inicial=100.0))
    assert metricas["pontos_serie"] == 60
    assert metricas["liquidez_2meses"] == pytest.approx(100.0 * 1_000_000, rel=0.05)
    # Serie que alterna +1%/-1% tem desvio diario ~1% -> ~16% anualizado.
    assert metricas["volatilidade_anualizada"] == pytest.approx(
        0.01 * math.sqrt(252), rel=0.10
    )


def test_serie_vazia_ou_ausente():
    for entrada in (None, [], [{"close": None}], ["nao-e-dict"]):
        metricas = bm.metricas_de_serie(entrada)
        assert metricas["volatilidade_anualizada"] is None


def test_linhas_de_serie_converte_epoch_em_data():
    linhas = bm.linhas_de_serie("PETR4", serie_sintetica(3), data_coleta="2026-10-01")
    assert len(linhas) == 3
    assert linhas[0]["data_pregao"] == "2023-11-14"  # epoch 1_700_000_000 em UTC
    assert linhas[0]["ticker"] == "PETR4"
    assert linhas[0]["data_coleta"] == "2026-10-01"
    assert isinstance(linhas[0]["volume"], int)


def test_linhas_de_serie_cobre_o_schema(payload_petr4):
    linhas = bm.linhas_de_serie(
        "PETR4", payload_petr4["historicalDataPrice"], data_coleta="2026-10-01"
    )
    assert linhas
    assert set(linhas[0]) == set(esquemas.colunas("silver_precos_brapi"))
