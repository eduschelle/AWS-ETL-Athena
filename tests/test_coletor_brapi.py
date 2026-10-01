"""Testes das partes puras do coletor: universo, dados sinteticos e politica de retry."""

import json

import pytest

import brapi_mapeamento as bm
import coletor_brapi as cb
import esquemas


class RespostaFalsa:
    def __init__(self, status: int, corpo: dict | None = None):
        self.status_code = status
        self._corpo = corpo or {}
        self.text = json.dumps(self._corpo)

    def json(self) -> dict:
        return self._corpo


class SessaoFalsa:
    """Sessao HTTP de mentira que devolve respostas roteirizadas e conta as chamadas."""

    def __init__(self, respostas: list):
        self.respostas = list(respostas)
        self.chamadas = []

    def get(self, url, params=None, timeout=None):
        self.chamadas.append({"url": url, "params": params, "timeout": timeout})
        resposta = self.respostas.pop(0)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta


@pytest.fixture(autouse=True)
def sem_espera(monkeypatch):
    """Neutraliza o backoff para os testes nao gastarem segundos reais dormindo."""
    monkeypatch.setattr(cb.time, "sleep", lambda _: None)


# ---------------------------------------------------------------------------
# Universo de coleta
# ---------------------------------------------------------------------------
def test_modo_auto_sem_token_mistura_real_e_sintetico():
    universo = cb.resolver_universo(modo="auto", token="")
    assert len(universo) == len(cb.TICKERS_PADRAO)

    reais = [t for t, fonte in universo if fonte == "brapi"]
    sinteticos = [t for t, fonte in universo if fonte == "sintetico"]
    assert sorted(reais) == sorted(cb.TICKERS_LIVRES)
    assert len(sinteticos) == len(cb.TICKERS_PADRAO) - len(cb.TICKERS_LIVRES)


def test_modo_auto_com_token_usa_tudo_real():
    universo = cb.resolver_universo(modo="auto", token="um-token-qualquer")
    assert all(fonte == "brapi" for _, fonte in universo)
    assert len(universo) == len(cb.TICKERS_PADRAO)


def test_modo_livre_nao_gera_sintetico():
    universo = cb.resolver_universo(modo="livre", token="")
    assert [t for t, _ in universo] == cb.TICKERS_LIVRES
    assert all(fonte == "brapi" for _, fonte in universo)


def test_modo_sintetico_nao_faz_requisicao():
    universo = cb.resolver_universo(modo="sintetico", token="tem-token-mas-ignora")
    assert all(fonte == "sintetico" for _, fonte in universo)


def test_modo_invalido_falha_cedo():
    with pytest.raises(ValueError, match="BRAPI_MODO invalido"):
        cb.resolver_universo(modo="qualquer-coisa", token="")


def test_modo_vem_do_ambiente_quando_omitido(monkeypatch):
    monkeypatch.setenv("BRAPI_MODO", "livre")
    monkeypatch.setenv("BRAPI_TOKEN", "")
    assert len(cb.resolver_universo()) == len(cb.TICKERS_LIVRES)


def test_todo_ticker_livre_esta_no_universo_padrao():
    assert set(cb.TICKERS_LIVRES) <= set(cb.TICKERS_PADRAO)


def test_todo_ticker_tem_perfil_sintetico():
    """Sem perfil, o ticker cairia num default generico e perderia o setor."""
    assert set(cb.TICKERS_PADRAO) <= set(cb.PERFIL_SINTETICO)


# ---------------------------------------------------------------------------
# Dados sinteticos
# ---------------------------------------------------------------------------
def test_sintetico_e_deterministico():
    """Idempotencia: reprocessar uma particao nao pode mudar os numeros nem o ranking."""
    a = cb.gerar_sintetico("WEGE3", "2026-10-01")
    b = cb.gerar_sintetico("WEGE3", "2026-10-01")
    assert a == b


def test_sinteticos_diferem_entre_tickers():
    a = cb.gerar_sintetico("WEGE3", "2026-10-01")
    b = cb.gerar_sintetico("EGIE3", "2026-10-01")
    assert a["regularMarketPrice"] != b["regularMarketPrice"]


def test_sintetico_atravessa_o_mapeamento_e_fica_elegivel():
    """O payload sintetico usa o MESMO formato da API, exercitando o mesmo mapeamento."""
    payload = cb.gerar_sintetico("WEGE3", "2026-10-01")
    linha = bm.extrair_indicadores(payload, fonte="sintetico", data_coleta="2026-10-01")

    assert set(linha) == set(esquemas.colunas("silver_indicadores_fundamentalistas"))
    assert linha["elegivel_score"] is True
    assert linha["setor"] == "Bens Industriais"
    assert linha["preco_lucro"] is not None
    assert linha["roe"] is not None
    assert linha["volatilidade_anualizada"] is not None


def test_sintetico_de_banco_reproduz_os_nulos_do_setor():
    """Espelhar os nulos reais faz o perfil de pesos financeiro ser exercitado de verdade."""
    linha = bm.extrair_indicadores(
        cb.gerar_sintetico("BBDC4", "2026-10-01"), fonte="sintetico"
    )
    assert linha["setor_financeiro"] is True
    assert linha["liq_corrente"] is None
    assert linha["margem_ebit"] is None
    assert linha["ebitda"] is None
    assert linha["roe"] is not None


def test_sintetico_de_bdr_fica_inelegivel():
    linha = bm.extrair_indicadores(
        cb.gerar_sintetico("MUTC34", "2026-10-01"), fonte="sintetico"
    )
    assert linha["classe_ativo"] == "BDR"
    assert linha["elegivel_score"] is False


def test_historico_sintetico_tem_pontos_suficientes():
    payload = cb.gerar_sintetico("EGIE3", "2026-10-01")
    serie = payload["historicalDataPrice"]
    assert len(serie) == cb.PREGOES_HISTORICO
    assert len(serie) >= bm.MIN_PONTOS_SERIE
    assert all(p["close"] > 0 for p in serie)


# ---------------------------------------------------------------------------
# Politica de retry
# ---------------------------------------------------------------------------
def test_sucesso_devolve_primeiro_resultado():
    sessao = SessaoFalsa([RespostaFalsa(200, {"results": [{"symbol": "PETR4"}]})])
    payload, status = cb.coletar("PETR4", token="", sessao=sessao)
    assert status == 200
    assert payload == {"symbol": "PETR4"}
    assert len(sessao.chamadas) == 1


def test_erro_401_nao_e_retentado():
    """O caso mais importante: 10 dos 14 tickers dao 401 sem token.

    Retry cego em 401 multiplicaria por 3 um gasto de quota que nao tem chance de sucesso.
    """
    sessao = SessaoFalsa(
        [RespostaFalsa(401, {"error": True, "code": "MISSING_TOKEN"})]
    )
    payload, status = cb.coletar("WEGE3", token="", sessao=sessao)
    assert payload is None
    assert status == 401
    assert len(sessao.chamadas) == 1


@pytest.mark.parametrize("status", [403, 404, 400])
def test_outros_erros_definitivos_nao_sao_retentados(status):
    sessao = SessaoFalsa([RespostaFalsa(status, {})])
    _, devolvido = cb.coletar("XPTO3", token="", sessao=sessao)
    assert devolvido == status
    assert len(sessao.chamadas) == 1


def test_erro_429_e_retentado_e_pode_ter_sucesso():
    sessao = SessaoFalsa(
        [
            RespostaFalsa(429, {}),
            RespostaFalsa(200, {"results": [{"symbol": "PETR4"}]}),
        ]
    )
    payload, status = cb.coletar("PETR4", token="", sessao=sessao)
    assert status == 200
    assert payload is not None
    assert len(sessao.chamadas) == 2


def test_erro_500_persistente_esgota_as_tentativas():
    sessao = SessaoFalsa([RespostaFalsa(503, {})] * cb.TENTATIVAS_MAXIMAS)
    payload, status = cb.coletar("PETR4", token="", sessao=sessao)
    assert payload is None
    assert status == 503
    assert len(sessao.chamadas) == cb.TENTATIVAS_MAXIMAS


def test_falha_de_rede_e_retentada():
    sessao = SessaoFalsa(
        [
            TimeoutError("connection timed out"),
            RespostaFalsa(200, {"results": [{"symbol": "PETR4"}]}),
        ]
    )
    payload, _ = cb.coletar("PETR4", token="", sessao=sessao)
    assert payload is not None
    assert len(sessao.chamadas) == 2


def test_resposta_200_sem_results_nao_quebra():
    sessao = SessaoFalsa([RespostaFalsa(200, {"results": []})])
    payload, status = cb.coletar("PETR4", token="", sessao=sessao)
    assert payload is None
    assert status == 200


def test_token_e_historico_vao_na_mesma_requisicao():
    """Uma unica chamada traz fundamentos E 3 meses de serie: nunca gastar duas."""
    sessao = SessaoFalsa([RespostaFalsa(200, {"results": [{}]})])
    cb.coletar("PETR4", token="segredo", sessao=sessao)
    parametros = sessao.chamadas[0]["params"]
    assert parametros["token"] == "segredo"
    assert parametros["range"] == cb.INTERVALO_HISTORICO
    assert parametros["interval"] == cb.GRANULARIDADE_HISTORICO
    assert parametros["modules"] == cb.MODULOS


def test_sem_token_o_parametro_e_omitido():
    sessao = SessaoFalsa([RespostaFalsa(200, {"results": [{}]})])
    cb.coletar("PETR4", token="", sessao=sessao)
    assert "token" not in sessao.chamadas[0]["params"]


# ---------------------------------------------------------------------------
# Envelope do bronze e landing
# ---------------------------------------------------------------------------
def test_envelope_cobre_o_schema_do_bronze():
    envelope = cb.montar_envelope("PETR4", {"symbol": "PETR4"}, "brapi", 200, "2026-10-01")
    assert set(envelope) == set(esquemas.colunas("bronze_fundamentos"))
    # O payload cru fica preservado intacto, para permitir replay e auditoria.
    assert json.loads(envelope["payload_json"]) == {"symbol": "PETR4"}


def test_envelope_de_falha_guarda_o_status():
    envelope = cb.montar_envelope("WEGE3", None, "brapi", 401, "2026-10-01")
    assert envelope["payload_json"] is None
    assert envelope["http_status"] == 401


def test_caminho_da_landing_usa_particao_hive():
    caminho = cb.caminho_landing("2026-10-01", "PETR4")
    assert caminho == "landing/fundamentos/data_coleta=2026-10-01/PETR4.json"


def test_landing_fica_fora_do_warehouse():
    """`sync_partition_metadata` quebra com diretorios estranhos dentro do location."""
    assert cb.PREFIXO_LANDING.startswith("landing/")
    for tabela in esquemas.TABELAS:
        assert cb.PREFIXO_LANDING not in esquemas.localizacao(tabela)
