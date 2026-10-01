"""Testes do motor de score: bandas, percentil com grupo minimo e renormalizacao de nulls."""

import pytest

import pontuacao as p


def ativo(ticker="XPTO3", **campos) -> dict:
    """Ativo elegivel com todos os indicadores do score preenchidos em valores medianos."""
    base = {
        "ticker": ticker,
        "fonte": "brapi",
        "elegivel_score": True,
        "setor": "Energia",
        "setor_financeiro": False,
        "cotacao": 10.0,
        "lpa": 1.0,
        "vpa": 5.0,
        "dividend_yield_origem": "brapi",
        "preco_lucro": 10.0,
        "preco_valor_patrimonial": 2.0,
        "psr": 2.0,
        "preco_ativo": 1.0,
        "ev_ebit": 10.0,
        "roe": 0.15,
        "roic": 0.12,
        "margem_liquida": 0.10,
        "margem_ebit": 0.15,
        "divida_liquida_patrimonio": 1.0,
        "divida_liquida_ebitda": 2.0,
        "liq_corrente": 1.5,
        "dividend_yield": 0.05,
        "ebitda": 1_000.0,
    }
    base.update(campos)
    return base


# ---------------------------------------------------------------------------
# Bandas absolutas
# ---------------------------------------------------------------------------
def test_nota_banda_nos_extremos_e_no_meio():
    assert p.nota_banda(4.0, 25.0, 4.0) == 100.0
    assert p.nota_banda(25.0, 25.0, 4.0) == 0.0
    assert p.nota_banda(14.5, 25.0, 4.0) == pytest.approx(50.0)


def test_nota_banda_limita_fora_da_faixa():
    """Valores melhores que o ponto 'bom' nao ganham mais de 100 — nem menos de 0."""
    assert p.nota_banda(1.0, 25.0, 4.0) == 100.0
    assert p.nota_banda(400.0, 25.0, 4.0) == 0.0


def test_nota_banda_direcao_crescente():
    assert p.nota_banda(0.25, 0.0, 0.25) == 100.0
    assert p.nota_banda(0.0, 0.0, 0.25) == 0.0


def test_nota_banda_rejeita_referencias_iguais():
    with pytest.raises(ValueError):
        p.nota_banda(1.0, 2.0, 2.0)


@pytest.mark.parametrize("indicador", p.NAO_POSITIVO_E_PESSIMO)
def test_multiplo_nao_positivo_recebe_nota_zero(indicador):
    """P/L negativo nao e 'barato': significa prejuizo. A razao perde sentido economico."""
    assert p.nota_indicador(indicador, -5.0) == 0.0
    assert p.nota_indicador(indicador, 0.0) == 0.0


def test_caixa_liquido_recebe_nota_maxima():
    assert p.nota_indicador("divida_liquida_patrimonio", -0.5) == 100.0


def test_divida_sobre_ebitda_negativa_por_ebitda_negativo_e_pessima():
    """Sinal enganoso: o indicador fica negativo porque o DENOMINADOR e negativo."""
    linha = ativo(divida_liquida_ebitda=-2.0, ebitda=-500.0)
    assert p.nota_indicador("divida_liquida_ebitda", -2.0, linha) == 0.0
    # Com EBITDA positivo, negativo e genuinamente caixa liquido.
    linha_ok = ativo(divida_liquida_ebitda=-2.0, ebitda=500.0)
    assert p.nota_indicador("divida_liquida_ebitda", -2.0, linha_ok) == 100.0


def test_dy_muito_alto_pontua_cheio_mas_e_sinalizado():
    assert p.nota_indicador("dividend_yield", 0.30) == 100.0
    assert p.flags_classicas(ativo(dividend_yield=0.30))["flag_dy_suspeito"] is True
    assert p.flags_classicas(ativo(dividend_yield=0.05))["flag_dy_suspeito"] is False


def test_indicador_nulo_ou_desconhecido_nao_tem_nota():
    assert p.nota_indicador("preco_lucro", None) is None
    assert p.nota_indicador("indicador_inexistente", 1.0) is None


# ---------------------------------------------------------------------------
# Percentil
# ---------------------------------------------------------------------------
def test_percentil_com_amostra_unitaria_e_neutro():
    """Com n=1, `percent_rank` do SQL daria 0.0; midrank devolve 0.5."""
    assert p.percentil_midrank(10.0, [10.0], melhor_maior=True) == 0.5


def test_percentil_distribui_empates():
    assert p.percentil_midrank(5.0, [5.0, 5.0, 5.0, 5.0], melhor_maior=True) == 0.5


def test_percentil_respeita_a_direcao():
    valores = [1.0, 2.0, 3.0, 4.0, 5.0]
    # Melhor maior: o 5 esta no topo.
    assert p.percentil_midrank(5.0, valores, melhor_maior=True) == 0.9
    # Melhor menor: o 5 esta no fundo.
    assert p.percentil_midrank(5.0, valores, melhor_maior=False) == 0.1


def test_percentil_sem_amostra():
    assert p.percentil_midrank(1.0, [], melhor_maior=True) is None
    assert p.percentil_midrank(None, [1.0], melhor_maior=True) is None


# ---------------------------------------------------------------------------
# Grupo de comparacao em cascata
# ---------------------------------------------------------------------------
def test_grupo_cai_para_universo_quando_setor_e_pequeno():
    universo = [
        ativo("AAAA3", setor="Energia"),
        ativo("BBBB3", setor="Mineracao"),
    ]
    grupo, rotulo = p.escolher_grupo(universo[0], universo)
    assert rotulo == "universo"
    assert len(grupo) == 2


def test_grupo_usa_setor_quando_ha_massa_critica():
    universo = [ativo(f"AAA{i}3", setor="Energia") for i in range(p.MIN_GRUPO)]
    universo.append(ativo("ZZZZ3", setor="Mineracao"))
    grupo, rotulo = p.escolher_grupo(universo[0], universo)
    assert rotulo == "setor"
    assert len(grupo) == p.MIN_GRUPO


def test_grupo_usa_macro_quando_setor_e_pequeno_mas_macro_nao():
    """Cenario real das financeiras: setores pulverizados, mas o macro-grupo tem massa."""
    universo = [
        ativo(f"BANK{i}4", setor=f"Banco {i}", setor_financeiro=True)
        for i in range(p.MIN_GRUPO)
    ]
    universo += [ativo("PETR4", setor="Energia")]
    grupo, rotulo = p.escolher_grupo(universo[0], universo)
    assert rotulo == "macro"
    assert len(grupo) == p.MIN_GRUPO
    assert all(c["setor_financeiro"] for c in grupo)


def test_grupo_nunca_mistura_fontes():
    """Rankear dado real contra dado sintetico falsificaria o percentil dos dois lados."""
    universo = [ativo(f"AAA{i}3", fonte="brapi") for i in range(3)]
    universo += [ativo(f"BBB{i}3", fonte="sintetico") for i in range(8)]
    grupo, _ = p.escolher_grupo(universo[0], universo)
    assert len(grupo) == 3
    assert all(c["fonte"] == "brapi" for c in grupo)


def test_grupo_exclui_inelegiveis():
    universo = [ativo("AAAA3")] + [
        ativo(f"BBB{i}3", elegivel_score=False) for i in range(5)
    ]
    grupo, _ = p.escolher_grupo(universo[0], universo)
    assert len(grupo) == 1


def test_grupo_pequeno_nao_altera_a_nota_da_banda():
    """Com menos de MIN_GRUPO ativos, a nota tem que ser a banda pura."""
    universo = [ativo("AAAA3"), ativo("BBBB3", preco_lucro=25.0)]
    nota = p.nota_metrica("preco_lucro", universo[0], universo)
    assert nota == p.nota_indicador("preco_lucro", universo[0]["preco_lucro"])


def test_grupo_grande_mistura_banda_e_percentil():
    universo = [ativo(f"AAA{i}3", preco_lucro=4.0 + i) for i in range(p.MIN_GRUPO)]
    melhor = universo[0]  # P/L mais baixo do grupo
    banda = p.nota_indicador("preco_lucro", melhor["preco_lucro"])
    nota = p.nota_metrica("preco_lucro", melhor, universo)
    assert nota != banda
    esperado = p.PESO_BANDA * banda + p.PESO_PERCENTIL * (0.9 * 100.0)
    assert nota == pytest.approx(esperado)


# ---------------------------------------------------------------------------
# Renormalizacao de nulls
# ---------------------------------------------------------------------------
def test_pilar_com_peso_insuficiente_fica_nulo():
    """Solidez com apenas liq_corrente (peso 0.30 de 1.00) nao atinge o minimo de 0.50."""
    linha = ativo(divida_liquida_patrimonio=None, divida_liquida_ebitda=None)
    pilares = p.notas_por_pilar(linha, [linha])
    assert pilares["solidez"] is None


def test_pilar_com_peso_suficiente_renormaliza():
    """Com dlp (0.40) + liq_corrente (0.30) = 0.70, o pilar sai — sobre os pesos presentes."""
    linha = ativo(divida_liquida_ebitda=None)
    pilares = p.notas_por_pilar(linha, [linha])
    assert pilares["solidez"] is not None

    nota_dlp = p.nota_indicador("divida_liquida_patrimonio", linha["divida_liquida_patrimonio"])
    nota_lc = p.nota_indicador("liq_corrente", linha["liq_corrente"])
    esperado = (0.40 * nota_dlp + 0.30 * nota_lc) / 0.70
    assert pilares["solidez"] == pytest.approx(esperado)


def test_null_nunca_e_tratado_como_zero():
    """Um indicador ausente nao pode arrastar a nota do pilar para baixo."""
    completo = ativo()
    sem_um = ativo(divida_liquida_ebitda=None)
    nota_completa = p.notas_por_pilar(completo, [completo])["solidez"]
    nota_parcial = p.notas_por_pilar(sem_um, [sem_um])["solidez"]
    # dle=2.0 recebe nota ~43; retirando-o, a media dos restantes muda, mas nao desaba.
    assert nota_parcial > nota_completa * 0.5


def test_score_sai_com_dois_pilares_e_nao_sai_com_um():
    # Valuation (0.30) + Rentabilidade (0.30) = 0.60, exatamente no minimo.
    com_dois = ativo(
        divida_liquida_patrimonio=None,
        divida_liquida_ebitda=None,
        liq_corrente=None,
        dividend_yield=None,
    )
    resultado = p.pontuar([com_dois])[0]
    assert resultado["score_final"] is not None
    assert resultado["cobertura_indicadores_pct"] == pytest.approx(0.60)
    assert resultado["confianca"] == "media"

    # Apenas Valuation (0.30), abaixo do minimo de 0.60.
    com_um = ativo(
        roe=None,
        roic=None,
        margem_liquida=None,
        margem_ebit=None,
        divida_liquida_patrimonio=None,
        divida_liquida_ebitda=None,
        liq_corrente=None,
        dividend_yield=None,
    )
    resultado = p.pontuar([com_um])[0]
    assert resultado["score_final"] is None
    assert resultado["nota"] == "SD"
    assert resultado["confianca"] == "baixa"


def test_cobertura_tipica_sem_dividend_yield():
    """Caso mais comum: DY ausente derruba 0.15, o score renormaliza sobre 0.85."""
    resultado = p.pontuar([ativo(dividend_yield=None)])[0]
    assert resultado["cobertura_indicadores_pct"] == pytest.approx(0.85)
    assert resultado["confianca"] == "alta"
    assert resultado["score_dividendos"] is None
    assert resultado["score_final"] is not None


# ---------------------------------------------------------------------------
# Perfil de pesos
# ---------------------------------------------------------------------------
def test_banco_usa_perfil_financeiro_e_nao_perde_solidez():
    """Sem o perfil financeiro, liq_corrente e dl/EBITDA nulos zerariam o pilar de solidez."""
    banco = ativo(
        "ITUB4",
        setor_financeiro=True,
        liq_corrente=None,
        divida_liquida_ebitda=None,
        margem_ebit=None,
    )
    resultado = p.pontuar([banco])[0]
    assert resultado["perfil_pesos"] == "financeiro"
    assert resultado["score_solidez"] is not None
    assert resultado["score_rentabilidade"] is not None
    assert resultado["cobertura_indicadores_pct"] == pytest.approx(1.0)


def test_pesos_de_cada_pilar_somam_um():
    assert sum(p.PESOS_PILAR.values()) == pytest.approx(1.0)
    for perfil, pilares in p.PESOS_METRICA.items():
        for pilar, metricas in pilares.items():
            assert sum(metricas.values()) == pytest.approx(
                1.0
            ), f"{perfil}/{pilar} nao soma 1.0"


def test_toda_metrica_pontuada_tem_banda():
    for pilares in p.PESOS_METRICA.values():
        for metricas in pilares.values():
            for nome in metricas:
                assert nome in p.BANDAS, f"{nome} nao tem banda cadastrada"


# ---------------------------------------------------------------------------
# Nota e letras
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "score,letra",
    [
        (100.0, "A"),
        (80.0, "A"),
        (79.99, "B"),
        (65.0, "B"),
        (50.0, "C"),
        (35.0, "D"),
        (34.99, "E"),
        (0.0, "E"),
        (None, "SD"),
    ],
)
def test_nota_letra_usa_cortes_fixos(score, letra):
    assert p.nota_letra(score) == letra


def test_universo_ruim_recebe_varias_notas_baixas():
    """Com quintis, 20% viraria 'A' de qualquer jeito. Com cortes fixos, nao."""
    ruins = [
        ativo(
            f"RUIM{i}3",
            preco_lucro=40.0,
            preco_valor_patrimonial=6.0,
            psr=8.0,
            preco_ativo=3.0,
            ev_ebit=35.0,
            roe=-0.05,
            roic=-0.03,
            margem_liquida=-0.10,
            margem_ebit=-0.05,
            divida_liquida_patrimonio=3.0,
            divida_liquida_ebitda=6.0,
            liq_corrente=0.5,
            dividend_yield=0.0,
        )
        for i in range(6)
    ]
    notas = {r["nota"] for r in p.pontuar(ruins)}
    assert notas == {"E"}


# ---------------------------------------------------------------------------
# Formulas classicas
# ---------------------------------------------------------------------------
def test_graham_e_margem_de_seguranca():
    flags = p.flags_classicas(ativo(cotacao=10.0, lpa=1.0, vpa=5.0))
    # sqrt(22.5 * 1 * 5) = sqrt(112.5) ~= 10.6066
    assert flags["valor_graham"] == pytest.approx(10.6066, abs=1e-3)
    assert flags["margem_seguranca"] == pytest.approx(0.06066, abs=1e-4)
    assert flags["flag_graham"] is True


def test_graham_nao_se_aplica_a_prejuizo():
    flags = p.flags_classicas(ativo(lpa=-1.0))
    assert flags["valor_graham"] is None
    assert flags["flag_graham"] is None


def test_bazin_so_com_dy_da_api():
    """Com `dividends` bloqueado no plano free, um DY aproximado produziria preco-teto falso."""
    com_dy = p.flags_classicas(
        ativo(cotacao=10.0, dividend_yield=0.09, dividend_yield_origem="brapi")
    )
    # (10 * 0.09) / 0.06 = 15.0
    assert com_dy["preco_teto_bazin"] == pytest.approx(15.0)
    assert com_dy["flag_bazin"] is True

    sem_origem = p.flags_classicas(
        ativo(dividend_yield=0.09, dividend_yield_origem="indisponivel")
    )
    assert sem_origem["preco_teto_bazin"] is None
    assert sem_origem["flag_bazin"] is None


def test_flags_de_screening():
    flags = p.flags_classicas(
        ativo(
            preco_lucro=8.0,
            preco_valor_patrimonial=1.2,
            roe=0.22,
            divida_liquida_patrimonio=0.5,
            liq_corrente=2.0,
        )
    )
    assert flags["flag_pl_barato"] is True
    assert flags["flag_pvp_barato"] is True
    assert flags["flag_roe_bom"] is True
    assert flags["flag_endiv_ok"] is True
    assert flags["flag_liquidez_ok"] is True


def test_pl_negativo_nao_conta_como_barato():
    assert p.flags_classicas(ativo(preco_lucro=-3.0))["flag_pl_barato"] is False


def test_magic_formula_soma_os_dois_ranks():
    universo = [
        ativo("AAAA3", ev_ebit=4.0, roic=0.30),  # melhor em ambos -> 1 + 1 = 2
        ativo("BBBB3", ev_ebit=10.0, roic=0.20),
        ativo("CCCC3", ev_ebit=20.0, roic=0.05),  # pior em ambos -> 3 + 3 = 6
    ]
    por_ticker = {r["ticker"]: r for r in p.pontuar(universo)}
    assert por_ticker["AAAA3"]["rank_magic_formula"] == 2
    assert por_ticker["CCCC3"]["rank_magic_formula"] == 6


def test_magic_formula_ignora_sem_dados():
    universo = [ativo("AAAA3", ev_ebit=None), ativo("BBBB3", roic=None)]
    for resultado in p.pontuar(universo):
        assert resultado["rank_magic_formula"] is None


# ---------------------------------------------------------------------------
# Integracao do pontuar()
# ---------------------------------------------------------------------------
def test_inelegivel_recebe_sd_sem_score():
    resultado = p.pontuar([ativo(elegivel_score=False)])[0]
    assert resultado["score_final"] is None
    assert resultado["nota"] == "SD"
    assert resultado["grupo_usado"] is None
    assert resultado["n_grupo"] == 0


def test_pontuar_preserva_campos_de_entrada():
    entrada = ativo("PETR4", setor="Energia")
    resultado = p.pontuar([entrada])[0]
    assert resultado["ticker"] == "PETR4"
    assert resultado["setor"] == "Energia"
    assert resultado["cotacao"] == 10.0


def test_pontuar_registra_como_o_score_foi_feito():
    """Auditabilidade: o grupo efetivamente usado precisa aparecer no resultado."""
    resultado = p.pontuar([ativo()])[0]
    assert resultado["grupo_usado"] == "universo"
    assert resultado["n_grupo"] == 1
    assert resultado["perfil_pesos"] == "padrao"
    assert 0.0 <= resultado["score_final"] <= 100.0


def test_pontuar_nao_muta_a_entrada():
    entrada = ativo()
    copia = dict(entrada)
    p.pontuar([entrada])
    assert entrada == copia


def test_pontuar_lista_vazia():
    assert p.pontuar([]) == []


# ---------------------------------------------------------------------------
# Sinal tecnico
# ---------------------------------------------------------------------------
def test_sinal_forte_exige_score_preco_e_confianca():
    base = dict(score_final=70.0, margem_seguranca=0.5, confianca="alta", elegivel_score=True)
    assert p.sinal_tecnico(base) == p.SINAL_FORTE

    # Score alto mas negociando ACIMA do valor intrinseco: bom negocio, preco ruim.
    assert p.sinal_tecnico({**base, "margem_seguranca": -0.02}) == p.SINAL_NEUTRO
    # Score alto mas dados fracos: nao emitimos sinal forte sobre pouca informacao.
    assert p.sinal_tecnico({**base, "confianca": "baixa"}) == p.SINAL_NEUTRO
    # Score insuficiente.
    assert p.sinal_tecnico({**base, "score_final": 64.9}) == p.SINAL_NEUTRO


def test_sinal_forte_sem_margem_calculavel():
    """Margem de Graham nula nao bloqueia: exigi-la puniria setores onde LPA/VPA nao se aplicam."""
    linha = dict(score_final=70.0, margem_seguranca=None, confianca="alta", elegivel_score=True)
    assert p.sinal_tecnico(linha) == p.SINAL_FORTE


def test_sinal_fraco_basta_uma_condicao_ruim():
    """Assimetria deliberada: FRACO com um alerta; FORTE exige tudo bom."""
    assert p.sinal_tecnico(dict(score_final=49.9, elegivel_score=True)) == p.SINAL_FRACO
    # Score excelente, mas preco muito acima do valor intrinseco.
    caro = dict(score_final=90.0, margem_seguranca=-0.25, confianca="alta", elegivel_score=True)
    assert p.sinal_tecnico(caro) == p.SINAL_FRACO


def test_sinal_neutro_na_faixa_do_meio():
    linha = dict(score_final=55.0, margem_seguranca=0.1, confianca="alta", elegivel_score=True)
    assert p.sinal_tecnico(linha) == p.SINAL_NEUTRO


def test_sinal_sem_dados():
    assert p.sinal_tecnico({"score_final": None}) == p.SINAL_SEM_DADOS
    assert p.sinal_tecnico({}) == p.SINAL_SEM_DADOS
    # Inelegivel nunca recebe sinal, mesmo que algum score tenha vazado.
    inelegivel = dict(score_final=80.0, elegivel_score=False)
    assert p.sinal_tecnico(inelegivel) == p.SINAL_SEM_DADOS


def test_sinal_cobre_todo_o_universo_pontuado():
    """Todo ativo recebe exatamente um dos quatro rotulos — nunca None."""
    universo = [ativo(f"AAA{i}3", preco_lucro=4.0 + i * 5) for i in range(6)]
    validos = {p.SINAL_FORTE, p.SINAL_NEUTRO, p.SINAL_FRACO, p.SINAL_SEM_DADOS}
    for linha in p.pontuar(universo):
        assert p.sinal_tecnico(linha) in validos
