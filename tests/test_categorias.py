"""Testes da classificacao por categoria."""

import pytest

import categorias as c


@pytest.mark.parametrize(
    "ticker,classe",
    [
        ("VALE3", "ACAO_ON"),
        ("MGLU3", "ACAO_ON"),
        ("PETR4", "ACAO_PN"),
        ("ITSA4", "ACAO_PN"),
        ("ITUB4", "ACAO_PN"),
        ("BRAV4", "ACAO_PN"),
        ("TIET5", "ACAO_PN"),
        ("MUTC34", "BDR"),  # recibo de empresa estrangeira
        ("AAPL34", "BDR"),
        ("BOVA11", "UNIT_OU_FII"),
        ("KNRI11", "UNIT_OU_FII"),
        ("", "OUTRO"),
        ("SEMNUMERO", "OUTRO"),
    ],
)
def test_classe_ativo_pelo_sufixo(ticker, classe):
    assert c.classe_ativo_do_ticker(ticker) == classe


def test_classe_ativo_normaliza_entrada():
    assert c.classe_ativo_do_ticker("  petr4  ") == "ACAO_PN"


@pytest.mark.parametrize(
    "setor,industria,esperado",
    [
        ("Servicos Financeiros", "Bancos - Regional", True),
        ("Financeiro", None, True),
        (None, "Seguradoras", True),
        ("Previdencia Privada", None, True),
        ("Energia", "Petroleo e Gas Integrado", False),
        ("Mineracao", None, False),
        (None, None, False),
    ],
)
def test_deteccao_de_setor_financeiro(setor, industria, esperado):
    assert c.eh_setor_financeiro(setor, industria) is esperado


@pytest.mark.parametrize(
    "valor,porte",
    [
        (500_000_000, "MICRO"),
        (1_999_999_999, "MICRO"),
        (2_000_000_000, "SMALL"),
        (9_999_999_999, "SMALL"),
        (10_000_000_000, "MID"),
        (49_999_999_999, "MID"),
        (50_000_000_000, "LARGE"),
        (633_000_000_000, "LARGE"),  # PETR4
        (None, None),
        (0, None),
        (-1, None),
    ],
)
def test_porte_por_bandas_absolutas(valor, porte):
    assert c.porte_por_valor_mercado(valor) == porte


@pytest.mark.parametrize(
    "vol,risco",
    [
        (0.10, "BAIXO"),
        (0.249, "BAIXO"),
        (0.25, "MEDIO"),
        (0.283, "MEDIO"),  # PETR4
        (0.399, "MEDIO"),
        (0.40, "ALTO"),
        (1.20, "ALTO"),
        (None, None),
    ],
)
def test_perfil_de_risco_por_volatilidade(vol, risco):
    assert c.perfil_risco(vol) == risco


def test_perfil_dividendos_exige_dy_alto_e_risco_controlado():
    assert c.perfil_investidor(0.08, "BAIXO", None, None, None) == "DIVIDENDOS"
    assert c.perfil_investidor(0.08, "MEDIO", None, None, None) == "DIVIDENDOS"
    # DY alto mas volatilidade alta nao e tese de dividendos.
    assert c.perfil_investidor(0.08, "ALTO", None, None, None) != "DIVIDENDOS"
    # DY insuficiente.
    assert c.perfil_investidor(0.03, "BAIXO", None, None, None) != "DIVIDENDOS"


def test_perfil_valor_exige_valuation_e_solidez():
    assert c.perfil_investidor(None, "MEDIO", 75.0, 70.0, None) == "VALOR"
    # Barato mas financeiramente fragil nao e "valor", e risco.
    assert c.perfil_investidor(None, "MEDIO", 75.0, 40.0, None) != "VALOR"


def test_perfil_crescimento():
    assert c.perfil_investidor(None, "ALTO", 20.0, 20.0, 0.25) == "CRESCIMENTO"
    # Crescimento forte com volatilidade baixa cai no fallback (perfil atipico).
    assert c.perfil_investidor(None, "BAIXO", 20.0, 20.0, 0.25) == "ESPECULATIVO"


def test_perfil_fallback_sem_nenhum_dado():
    assert c.perfil_investidor(None, None, None, None, None) == "ESPECULATIVO"


def test_ordem_de_precedencia_dividendos_vence_valor():
    """Um ativo que satisfaz as duas teses e rotulado pela mais restritiva."""
    assert c.perfil_investidor(0.09, "BAIXO", 90.0, 90.0, None) == "DIVIDENDOS"


def test_classificar_aplica_tudo_de_uma_vez():
    resultado = c.classificar(
        {
            "ticker": "PETR4",
            "valor_mercado": 633_000_000_000,
            "volatilidade_anualizada": 0.283,
            "dividend_yield": 0.07,
            "score_valuation": 85.0,
            "score_solidez": 55.0,
            "cresc_receita_12m": 0.11,
        }
    )
    assert resultado == {
        "classe_ativo": "ACAO_PN",
        "porte": "LARGE",
        "perfil_risco": "MEDIO",
        "perfil_investidor": "DIVIDENDOS",
    }


def test_classificar_respeita_classe_ja_definida():
    """A classe vinda do silver tem precedencia sobre a reinferencia pelo ticker."""
    resultado = c.classificar({"ticker": "PETR4", "classe_ativo": "ACAO_PN"})
    assert resultado["classe_ativo"] == "ACAO_PN"


def test_classificar_com_dados_ausentes_nao_explode():
    resultado = c.classificar({"ticker": "XPTO3"})
    assert resultado["porte"] is None
    assert resultado["perfil_risco"] is None
    assert resultado["perfil_investidor"] == "ESPECULATIVO"


def test_inelegivel_nao_recebe_rotulo_de_tese():
    """Ausencia de dados nao e a mesma coisa que perfil especulativo."""
    resultado = c.classificar({"ticker": "MUTC34", "elegivel_score": False})
    assert resultado["perfil_investidor"] is None
    # Classe continua inferivel: e um fato sobre o ticker, nao uma avaliacao.
    assert resultado["classe_ativo"] == "BDR"


def test_elegivel_recebe_rotulo():
    resultado = c.classificar({"ticker": "PETR4", "elegivel_score": True})
    assert resultado["perfil_investidor"] == "ESPECULATIVO"


def test_sem_campo_de_elegibilidade_assume_classificavel():
    resultado = c.classificar({"ticker": "PETR4"})
    assert resultado["perfil_investidor"] is not None
