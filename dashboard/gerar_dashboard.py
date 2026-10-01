"""
Gera um dashboard HTML autocontido a partir da tabela gold, para abrir no navegador.

Por que HTML estatico e nao uma ferramenta de BI: zero infraestrutura nova, o arquivo e
versionavel, abre offline e nao depende de CDN. O custo e ser um retrato — para atualizar,
rode de novo. Se o projeto evoluir para dashboards colaborativos com filtros salvos, a troca
natural e um Metabase ligado ao Trino.

Consulta o Trino pelo CLI que ja existe dentro do container (`--output-format JSON`), entao
nao exige nenhuma dependencia Python nova no host.

Uso (na raiz do projeto):
    python dashboard/gerar_dashboard.py
    python dashboard/gerar_dashboard.py --data-referencia 2026-09-28 --saida dashboard/d.html
"""

import argparse
import html
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "spark" / "jobs"))

import pontuacao  # noqa: E402

SAIDA_PADRAO = RAIZ / "dashboard" / "dashboard.html"

# --- Paleta -------------------------------------------------------------------------
# Instancia da paleta de referencia validada. As cores de STATUS sao fixas e, no modo claro,
# o amarelo (NEUTRO) fica abaixo de 3:1 contra a superficie por desenho — a mitigacao
# obrigatoria e icone + rotulo visivel, nunca cor sozinha, mais a propria tabela como
# table view. Ambos estao implementados abaixo.
STATUS = {
    pontuacao.SINAL_FORTE: {"cor": "good", "icone": "▲", "rotulo": "FORTE"},
    pontuacao.SINAL_NEUTRO: {"cor": "warning", "icone": "●", "rotulo": "NEUTRO"},
    pontuacao.SINAL_FRACO: {"cor": "critical", "icone": "▼", "rotulo": "FRACO"},
    pontuacao.SINAL_SEM_DADOS: {"cor": "muted", "icone": "—", "rotulo": "SEM DADOS"},
}
ORDEM_SINAL = [
    pontuacao.SINAL_FORTE,
    pontuacao.SINAL_NEUTRO,
    pontuacao.SINAL_FRACO,
    pontuacao.SINAL_SEM_DADOS,
]

# Rampa sequencial de UM hue (azul), mais-e-mais-escuro no claro e mais-e-mais-claro no
# escuro. Seis degraus; o rotulo dentro da celula troca de tinta para manter contraste.
FAIXAS_HEATMAP = [20.0, 40.0, 55.0, 70.0, 85.0]

PILARES = [
    ("score_valuation", "Valuation"),
    ("score_rentabilidade", "Rentabilidade"),
    ("score_solidez", "Solidez"),
    ("score_dividendos", "Dividendos"),
]

CONSULTA = """
SELECT
    g.ticker, g.nota, g.score_final, g.score_valuation, g.score_rentabilidade,
    g.score_solidez, g.score_dividendos, g.setor, g.porte, g.perfil_risco,
    g.perfil_investidor, g.confianca, g.cobertura_indicadores_pct, g.grupo_usado,
    g.n_grupo, g.margem_seguranca, g.valor_graham, g.preco_teto_bazin, g.cotacao,
    g.elegivel_score, g.fonte, g.rank_magic_formula, g.data_referencia,
    i.preco_lucro, i.preco_valor_patrimonial, i.dividend_yield, i.roe,
    i.divida_liquida_patrimonio, i.liquidez_2meses
FROM hive.default.gold_classificacao_ativos AS g
LEFT JOIN hive.default.silver_indicadores_fundamentalistas AS i
       ON i.ticker = g.ticker AND i.data_coleta = g.data_referencia
WHERE g.data_referencia = {filtro_data}
"""


def consultar(data_referencia: str | None) -> list[dict]:
    """Executa a consulta via CLI do Trino dentro do container e devolve as linhas."""
    filtro = (
        f"'{data_referencia}'"
        if data_referencia
        else "(SELECT max(data_referencia) FROM hive.default.gold_classificacao_ativos)"
    )
    sql = " ".join(CONSULTA.format(filtro_data=filtro).split())

    comando = [
        "docker", "exec", "trino", "trino",
        "--catalog", "hive", "--schema", "default",
        "--output-format", "JSON", "--execute", sql,
    ]
    processo = subprocess.run(comando, capture_output=True, text=True, encoding="utf-8")
    if processo.returncode != 0:
        raise SystemExit(
            "Falha ao consultar o Trino. O container esta no ar?\n"
            f"  docker compose ps trino\n\n{processo.stderr.strip()}"
        )

    linhas = [
        json.loads(linha)
        for linha in processo.stdout.splitlines()
        if linha.strip().startswith("{")
    ]
    if not linhas:
        raise SystemExit(
            "A consulta nao retornou linhas. A DAG pipeline_b3_classificacao_investimentos "
            "ja rodou com sucesso?"
        )
    return linhas


# --- Formatacao ---------------------------------------------------------------------
def n(valor, casas: int = 1, sufixo: str = "") -> str:
    if valor is None:
        return "&ndash;"
    return f"{valor:,.{casas}f}".replace(",", " ") + sufixo


def pct(valor, casas: int = 0) -> str:
    return "&ndash;" if valor is None else f"{valor * 100:,.{casas}f}%"


def compacto(valor) -> str:
    """Formata valores financeiros grandes em mi/bi, para caber na tabela."""
    if valor is None:
        return "&ndash;"
    if abs(valor) >= 1e9:
        return f"{valor / 1e9:.1f} bi"
    if abs(valor) >= 1e6:
        return f"{valor / 1e6:.0f} mi"
    return f"{valor:,.0f}".replace(",", " ")


def esc(valor) -> str:
    return "&ndash;" if valor in (None, "") else html.escape(str(valor))


def faixa_heatmap(valor) -> int:
    """Indice do degrau da rampa (1..6), ou 0 para ausencia de dado."""
    if valor is None:
        return 0
    for i, limite in enumerate(FAIXAS_HEATMAP):
        if valor < limite:
            return i + 1
    return len(FAIXAS_HEATMAP) + 1


# --- Componentes -------------------------------------------------------------------
def tile(valor: str, rotulo: str, nota: str = "", classe: str = "") -> str:
    extra = f'<div class="tile-nota">{nota}</div>' if nota else ""
    return (
        f'<div class="tile {classe}">'
        f'<div class="tile-valor">{valor}</div>'
        f'<div class="tile-rotulo">{rotulo}</div>{extra}</div>'
    )


def selo_sinal(sinal: str) -> str:
    """Status sempre com icone + rotulo textual, nunca cor sozinha."""
    info = STATUS[sinal]
    return (
        f'<span class="selo selo-{info["cor"]}">'
        f'<span class="selo-icone" aria-hidden="true">{info["icone"]}</span>'
        f'{info["rotulo"]}</span>'
    )


def construir_kpis(linhas: list[dict]) -> str:
    contagem = {s: 0 for s in ORDEM_SINAL}
    for linha in linhas:
        contagem[linha["sinal"]] += 1

    reais = sum(1 for linha in linhas if linha.get("fonte") == "brapi")
    com_score = sum(1 for linha in linhas if linha.get("score_final") is not None)

    partes = [
        tile(str(len(linhas)), "Ativos no universo", f"{com_score} com score"),
        tile(
            f'{STATUS[pontuacao.SINAL_FORTE]["icone"]} {contagem[pontuacao.SINAL_FORTE]}',
            "Sinal forte", "score &ge; 65 e abaixo do valor", "tile-good",
        ),
        tile(
            f'{STATUS[pontuacao.SINAL_NEUTRO]["icone"]} {contagem[pontuacao.SINAL_NEUTRO]}',
            "Neutro", "sem tese clara", "tile-warning",
        ),
        tile(
            f'{STATUS[pontuacao.SINAL_FRACO]["icone"]} {contagem[pontuacao.SINAL_FRACO]}',
            "Sinal fraco", "score &lt; 50 ou caro", "tile-critical",
        ),
        tile(f"{reais}/{len(linhas)}", "Com dados reais", "o resto e sintetico"),
    ]
    return f'<section class="kpis">{"".join(partes)}</section>'


def construir_tabela(linhas: list[dict]) -> str:
    colunas = [
        ("Papel", ""), ("Sinal", ""), ("Nota", ""), ("Score", "num"),
        ("P/L", "num"), ("P/VP", "num"), ("DY", "num"), ("ROE", "num"),
        ("Dív.Líq/PL", "num"), ("Marg. seg.", "num"),
        ("Setor", ""), ("Porte", ""), ("Risco", ""), ("Perfil", ""),
        ("Confiança", ""), ("Fonte", ""),
    ]
    cabecalho = "".join(
        f'<th class="{classe}" scope="col">{nome}</th>' for nome, classe in colunas
    )

    corpo = []
    for linha in linhas:
        margem = linha.get("margem_seguranca")
        classe_margem = ""
        if margem is not None:
            classe_margem = "delta-pos" if margem > 0 else "delta-neg"

        dica = (
            f'Grupo de comparação: {esc(linha.get("grupo_usado"))} '
            f'(n={linha.get("n_grupo")}) · '
            f'cobertura {pct(linha.get("cobertura_indicadores_pct"))} · '
            f'liquidez {compacto(linha.get("liquidez_2meses"))}/pregão'
        )

        corpo.append(
            "<tr>"
            f'<th scope="row" class="papel">{esc(linha["ticker"])}</th>'
            f"<td>{selo_sinal(linha['sinal'])}</td>"
            f'<td><span class="nota nota-{esc(linha.get("nota"))}">{esc(linha.get("nota"))}</span></td>'
            f'<td class="num forte">{n(linha.get("score_final"))}</td>'
            f'<td class="num">{n(linha.get("preco_lucro"), 1)}</td>'
            f'<td class="num">{n(linha.get("preco_valor_patrimonial"), 2)}</td>'
            f'<td class="num">{pct(linha.get("dividend_yield"), 1)}</td>'
            f'<td class="num">{pct(linha.get("roe"), 1)}</td>'
            f'<td class="num">{n(linha.get("divida_liquida_patrimonio"), 2)}</td>'
            f'<td class="num {classe_margem}">{pct(margem)}</td>'
            f'<td>{esc(linha.get("setor"))}</td>'
            f'<td>{esc(linha.get("porte"))}</td>'
            f'<td>{esc(linha.get("perfil_risco"))}</td>'
            f'<td>{esc(linha.get("perfil_investidor"))}</td>'
            f'<td class="sec" data-dica="{dica}">{esc(linha.get("confianca"))}</td>'
            f'<td class="sec">{esc(linha.get("fonte"))}</td>'
            "</tr>"
        )

    return (
        '<section class="bloco"><h2>Ativos classificados</h2>'
        '<p class="sub">Ordenado por score. Passe o mouse na coluna '
        "<em>Confiança</em> para ver o grupo de comparação e a cobertura de dados.</p>"
        f'<div class="rolagem"><table><thead><tr>{cabecalho}</tr></thead>'
        f"<tbody>{''.join(corpo)}</tbody></table></div></section>"
    )


def construir_heatmap(linhas: list[dict]) -> str:
    """Grade ativo x pilar. Magnitude numa grade -> heatmap com rampa de um hue."""
    elegiveis = [linha for linha in linhas if linha.get("score_final") is not None]

    cabecalho = '<div class="hm-cel hm-canto"></div>' + "".join(
        f'<div class="hm-cel hm-topo">{rotulo}</div>' for _, rotulo in PILARES
    )

    corpo = []
    for linha in elegiveis:
        corpo.append(f'<div class="hm-cel hm-lado">{esc(linha["ticker"])}</div>')
        for campo, rotulo in PILARES:
            valor = linha.get(campo)
            faixa = faixa_heatmap(valor)
            dica = (
                f'{esc(linha["ticker"])} · {rotulo}: '
                + ("sem dados suficientes" if valor is None else f"{valor:.0f} de 100")
            )
            texto = "n/d" if valor is None else f"{valor:.0f}"
            corpo.append(
                f'<div class="hm-cel hm-val hm-{faixa}" data-dica="{dica}" '
                f'tabindex="0">{texto}</div>'
            )

    legenda = "".join(
        f'<span class="hm-leg-passo hm-{i}"></span>' for i in range(1, len(FAIXAS_HEATMAP) + 2)
    )

    return (
        '<section class="bloco"><h2>Score por pilar</h2>'
        '<p class="sub">Cada pilar vale de 0 a 100. Células vazias (<code>n/d</code>) são '
        "indicadores que a API não entrega para aquele tipo de empresa &mdash; bancos não têm "
        "liquidez corrente nem EBITDA no sentido industrial, por exemplo.</p>"
        f'<div class="heatmap" style="--cols:{len(PILARES)}">{cabecalho}{"".join(corpo)}</div>'
        f'<div class="hm-legenda"><span class="sec">0</span>{legenda}'
        '<span class="sec">100</span></div></section>'
    )


def construir_legenda_status() -> str:
    itens = "".join(
        f'<div class="leg-item">{selo_sinal(sinal)}<span class="sec">{desc}</span></div>'
        for sinal, desc in [
            (pontuacao.SINAL_FORTE, "score &ge; 65, preço abaixo do valor de Graham e dados confiáveis"),
            (pontuacao.SINAL_NEUTRO, "sem alerta, mas sem reunir todas as condições"),
            (pontuacao.SINAL_FRACO, "score &lt; 50 <em>ou</em> preço &gt; 20% acima do valor intrínseco"),
            (pontuacao.SINAL_SEM_DADOS, "não elegível (BDR, FII ou coleta sem retorno)"),
        ]
    )
    return f'<section class="legenda">{itens}</section>'


ESTILO = """
*,*::before,*::after{box-sizing:border-box}
.viz-root{
  color-scheme:light;
  --surface-1:#fcfcfb; --plane:#f9f9f7;
  --text-primary:#0b0b0b; --text-secondary:#52514e; --muted:#898781;
  --grid:#e1e0d9; --baseline:#c3c2b7; --border:rgba(11,11,11,0.10);
  --good:#0ca30c; --warning:#fab219; --critical:#d03b3b;
  --delta-pos:#006300; --delta-neg:#d03b3b;
  --hm-1-bg:#b7d3f6; --hm-1-fg:#0b0b0b;
  --hm-2-bg:#86b6ef; --hm-2-fg:#0b0b0b;
  --hm-3-bg:#5598e7; --hm-3-fg:#0b0b0b;
  --hm-4-bg:#2a78d6; --hm-4-fg:#ffffff;
  --hm-5-bg:#1c5cab; --hm-5-fg:#ffffff;
  --hm-6-bg:#104281; --hm-6-fg:#ffffff;
  --hm-0-bg:#f0efec; --hm-0-fg:#898781;
}
@media (prefers-color-scheme:dark){
  :root:where(:not([data-theme="light"])) .viz-root{
    color-scheme:dark;
    --surface-1:#1a1a19; --plane:#0d0d0d;
    --text-primary:#ffffff; --text-secondary:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --baseline:#383835; --border:rgba(255,255,255,0.10);
    --delta-pos:#0ca30c; --delta-neg:#e66767;
    --hm-1-bg:#184f95; --hm-1-fg:#ffffff;
    --hm-2-bg:#1c5cab; --hm-2-fg:#ffffff;
    --hm-3-bg:#2a78d6; --hm-3-fg:#ffffff;
    --hm-4-bg:#3987e5; --hm-4-fg:#0b0b0b;
    --hm-5-bg:#6da7ec; --hm-5-fg:#0b0b0b;
    --hm-6-bg:#9ec5f4; --hm-6-fg:#0b0b0b;
    --hm-0-bg:#383835; --hm-0-fg:#898781;
  }
}
:root[data-theme="dark"] .viz-root{
  color-scheme:dark;
  --surface-1:#1a1a19; --plane:#0d0d0d;
  --text-primary:#ffffff; --text-secondary:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --baseline:#383835; --border:rgba(255,255,255,0.10);
  --delta-pos:#0ca30c; --delta-neg:#e66767;
  --hm-1-bg:#184f95; --hm-1-fg:#ffffff;
  --hm-2-bg:#1c5cab; --hm-2-fg:#ffffff;
  --hm-3-bg:#2a78d6; --hm-3-fg:#ffffff;
  --hm-4-bg:#3987e5; --hm-4-fg:#0b0b0b;
  --hm-5-bg:#6da7ec; --hm-5-fg:#0b0b0b;
  --hm-6-bg:#9ec5f4; --hm-6-fg:#0b0b0b;
  --hm-0-bg:#383835; --hm-0-fg:#898781;
}
html,body{margin:0;padding:0}
body{background:var(--plane);color:var(--text-primary);
  font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
.viz-root{background:var(--plane);min-height:100vh;padding:28px 24px 48px}
.wrap{max-width:1360px;margin:0 auto}
header{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;
  margin-bottom:22px}
h1{font-size:21px;font-weight:650;margin:0 0 4px}
h2{font-size:15px;font-weight:620;margin:0 0 3px}
.sub{color:var(--text-secondary);margin:0 0 14px;font-size:12.5px;max-width:78ch}
.sec{color:var(--text-secondary)}
code{font-size:0.92em;background:var(--hm-0-bg);padding:1px 4px;border-radius:3px}
#tema{background:var(--surface-1);color:var(--text-secondary);cursor:pointer;
  border:1px solid var(--border);border-radius:7px;padding:7px 12px;font:inherit;font-size:12px}
#tema:hover{color:var(--text-primary)}
.bloco{background:var(--surface-1);border:1px solid var(--border);border-radius:11px;
  padding:18px 18px 20px;margin-bottom:18px}
/* KPI row */
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:12px;
  margin-bottom:18px}
.tile{background:var(--surface-1);border:1px solid var(--border);border-radius:11px;
  padding:15px 16px;border-left:3px solid var(--baseline)}
.tile-good{border-left-color:var(--good)}
.tile-warning{border-left-color:var(--warning)}
.tile-critical{border-left-color:var(--critical)}
.tile-valor{font-size:29px;font-weight:620;line-height:1.1;letter-spacing:-0.01em}
.tile-rotulo{font-size:12.5px;color:var(--text-secondary);margin-top:3px}
.tile-nota{font-size:11px;color:var(--muted);margin-top:2px}
/* legenda de status */
.legenda{display:flex;flex-wrap:wrap;gap:9px 26px;background:var(--surface-1);
  border:1px solid var(--border);border-radius:11px;padding:14px 18px;margin-bottom:18px;
  font-size:12.5px}
.leg-item{display:flex;align-items:center;gap:9px}
/* selo de status: cor + icone + rotulo, nunca cor sozinha */
.selo{display:inline-flex;align-items:center;gap:5px;font-size:11.5px;font-weight:600;
  letter-spacing:0.02em;white-space:nowrap}
.selo-icone{font-size:10px;line-height:1}
.selo-good{color:var(--good)} .selo-warning{color:var(--warning)}
.selo-critical{color:var(--critical)} .selo-muted{color:var(--muted)}
/* tabela */
.rolagem{overflow-x:auto;margin:0 -4px;padding:0 4px}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--grid);
  white-space:nowrap}
thead th{color:var(--muted);font-weight:600;font-size:11px;letter-spacing:0.03em;
  text-transform:uppercase;border-bottom:1px solid var(--baseline)}
tbody tr:last-child th,tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--plane)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.papel{font-weight:620}
.forte{font-weight:620}
.delta-pos{color:var(--delta-pos)} .delta-neg{color:var(--delta-neg)}
.nota{display:inline-block;min-width:19px;text-align:center;font-weight:620;font-size:11.5px}
.nota-A,.nota-B{color:var(--delta-pos)}
.nota-C{color:var(--text-secondary)}
.nota-D,.nota-E{color:var(--critical)}
.nota-None{color:var(--muted)}
/* heatmap: 2px de superficie entre celulas */
.heatmap{display:grid;grid-template-columns:auto repeat(var(--cols),minmax(84px,1fr));
  gap:2px;margin-bottom:12px}
.hm-cel{padding:7px 9px;font-size:12px;display:flex;align-items:center}
.hm-topo{color:var(--muted);font-size:10.5px;font-weight:600;letter-spacing:0.03em;
  text-transform:uppercase;justify-content:center;padding-bottom:5px}
.hm-lado{font-weight:620;font-size:12.5px;padding-right:12px}
.hm-val{justify-content:center;border-radius:4px;font-variant-numeric:tabular-nums;
  cursor:default;transition:outline-color 90ms}
.hm-val:hover,.hm-val:focus-visible{outline:2px solid var(--text-primary);
  outline-offset:1px}
.hm-0{background:var(--hm-0-bg);color:var(--hm-0-fg);font-size:10.5px;
  background-image:repeating-linear-gradient(45deg,transparent 0 4px,var(--border) 4px 5px)}
.hm-1{background:var(--hm-1-bg);color:var(--hm-1-fg)}
.hm-2{background:var(--hm-2-bg);color:var(--hm-2-fg)}
.hm-3{background:var(--hm-3-bg);color:var(--hm-3-fg)}
.hm-4{background:var(--hm-4-bg);color:var(--hm-4-fg)}
.hm-5{background:var(--hm-5-bg);color:var(--hm-5-fg)}
.hm-6{background:var(--hm-6-bg);color:var(--hm-6-fg)}
.hm-legenda{display:flex;align-items:center;gap:3px;font-size:11px}
.hm-legenda .sec{margin:0 6px}
.hm-leg-passo{width:30px;height:9px;border-radius:2px;background:var(--hm-1-bg)}
.hm-leg-passo.hm-2{background:var(--hm-2-bg)} .hm-leg-passo.hm-3{background:var(--hm-3-bg)}
.hm-leg-passo.hm-4{background:var(--hm-4-bg)} .hm-leg-passo.hm-5{background:var(--hm-5-bg)}
.hm-leg-passo.hm-6{background:var(--hm-6-bg)}
/* tooltip */
#dica{position:fixed;z-index:50;pointer-events:none;opacity:0;transition:opacity 90ms;
  background:var(--text-primary);color:var(--surface-1);font-size:11.5px;line-height:1.45;
  padding:6px 9px;border-radius:6px;max-width:300px;box-shadow:0 2px 10px rgba(0,0,0,0.22)}
#dica.on{opacity:1}
[data-dica]{cursor:help}
footer{color:var(--muted);font-size:11.5px;line-height:1.6;margin-top:22px;
  border-top:1px solid var(--grid);padding-top:14px;max-width:92ch}
footer strong{color:var(--text-secondary);font-weight:600}
@media print{.viz-root{padding:0}#tema{display:none}
  .bloco,.tile,.legenda{border-color:#999;break-inside:avoid}}
"""

SCRIPT = """
(function(){
  var raiz=document.documentElement, botao=document.getElementById('tema');
  function rotular(){
    var escuro=raiz.getAttribute('data-theme')==='dark'||
      (!raiz.hasAttribute('data-theme')&&matchMedia('(prefers-color-scheme:dark)').matches);
    botao.textContent=escuro?'\\u2600 Tema claro':'\\u263d Tema escuro';
  }
  botao.addEventListener('click',function(){
    var escuro=raiz.getAttribute('data-theme')==='dark'||
      (!raiz.hasAttribute('data-theme')&&matchMedia('(prefers-color-scheme:dark)').matches);
    raiz.setAttribute('data-theme',escuro?'light':'dark'); rotular();
  });
  rotular();

  var dica=document.getElementById('dica');
  function mostrar(e){
    var alvo=e.target.closest('[data-dica]'); if(!alvo)return;
    dica.innerHTML=alvo.getAttribute('data-dica'); dica.classList.add('on');
    var r=alvo.getBoundingClientRect(), d=dica.getBoundingClientRect();
    var x=Math.min(Math.max(8,r.left+r.width/2-d.width/2),innerWidth-d.width-8);
    var y=r.top-d.height-8; if(y<8){y=r.bottom+8;}
    dica.style.left=x+'px'; dica.style.top=y+'px';
  }
  function esconder(){dica.classList.remove('on');}
  document.addEventListener('mouseover',mostrar);
  document.addEventListener('mouseout',esconder);
  document.addEventListener('focusin',mostrar);
  document.addEventListener('focusout',esconder);
})();
"""


def montar_html(linhas: list[dict], data_referencia: str) -> str:
    gerado = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    reais = sum(1 for linha in linhas if linha.get("fonte") == "brapi")

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Classificação de investimentos &middot; B3</title>
<style>{ESTILO}</style>
</head>
<body>
<div class="viz-root">
<div class="wrap">

<header>
  <div>
    <h1>Classificação de investimentos &middot; B3</h1>
    <p class="sub">Data de referência <strong>{esc(data_referencia)}</strong> &middot;
      {len(linhas)} ativos &middot; gerado em {gerado}</p>
  </div>
  <button id="tema" type="button">Tema</button>
</header>

{construir_kpis(linhas)}
{construir_legenda_status()}
{construir_tabela(linhas)}
{construir_heatmap(linhas)}

<footer>
  <p><strong>Como ler o sinal.</strong> Ele combina qualidade (score composto de 4 pilares)
  com preço (margem de segurança sobre o valor de Graham). As regras são assimétricas de
  propósito: para marcar <em>FRACO</em> basta um alerta isolado, enquanto <em>FORTE</em> exige
  todas as condições boas ao mesmo tempo, inclusive confiança mínima nos dados.</p>
  <p><strong>Procedência dos dados.</strong> {reais} dos {len(linhas)} ativos têm fundamentos
  reais da brapi.dev; os demais são sintéticos, marcados como <code>sintetico</code> na coluna
  Fonte, e nunca são comparados contra os reais no cálculo de percentil. A cotação tem atraso
  de cerca de 30 minutos e alguns indicadores são derivados por aproximação, com a origem
  registrada na camada silver.</p>
  <p><strong>Isto não é recomendação de investimento.</strong> É a saída de um modelo
  construído para exercitar um pipeline de dados.</p>
</footer>

</div></div>
<div id="dica" role="tooltip"></div>
<script>{SCRIPT}</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-referencia",
        help="Partição a renderizar (padrão: a mais recente da tabela gold)",
    )
    parser.add_argument("--saida", type=Path, default=SAIDA_PADRAO)
    argumentos = parser.parse_args()

    linhas = consultar(argumentos.data_referencia)

    for linha in linhas:
        linha["sinal"] = pontuacao.sinal_tecnico(linha)

    # Ordem de leitura: melhor score primeiro, inelegiveis ao final.
    linhas.sort(key=lambda l: (l.get("score_final") is None, -(l.get("score_final") or 0)))
    data_referencia = linhas[0].get("data_referencia") or "n/d"

    argumentos.saida.parent.mkdir(parents=True, exist_ok=True)
    argumentos.saida.write_text(montar_html(linhas, data_referencia), encoding="utf-8")

    contagem = {}
    for linha in linhas:
        contagem[linha["sinal"]] = contagem.get(linha["sinal"], 0) + 1

    print(f"Dashboard gerado: {argumentos.saida}", file=sys.stderr)
    print(f"  data_referencia={data_referencia}  ativos={len(linhas)}", file=sys.stderr)
    print(f"  sinais={contagem}", file=sys.stderr)


if __name__ == "__main__":
    main()
