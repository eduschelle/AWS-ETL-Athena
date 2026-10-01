-- ============================================================
-- Consultas sobre o sistema de classificação de investimentos
-- (camadas bronze/silver/gold alimentadas pela brapi.dev)
--
-- Como rodar (veja detalhes no README):
--   cmd /c "docker exec -i trino trino --catalog hive --schema default < sql\classificacao.sql"
--
-- Atenção no PowerShell: NÃO use `Get-Content ... | docker exec`, o pipe insere um BOM UTF-8
-- que quebra o parser do Trino ("mismatched input"). Use `cmd /c` com `<`, como acima.
-- ============================================================

-- 1) Sanity check: quantos ativos foram classificados em cada execução?
--    Como a tabela gold é um snapshot acumulativo, cada data_referencia é uma rodada semanal.
SELECT
    data_referencia,
    COUNT(*) AS ativos,
    COUNT(score_final) AS com_score,
    COUNT(*) FILTER (WHERE fonte = 'brapi') AS dados_reais,
    COUNT(*) FILTER (WHERE fonte = 'sintetico') AS dados_sinteticos
FROM hive.default.gold_classificacao_ativos
GROUP BY data_referencia
ORDER BY data_referencia DESC;

-- 2) O ranking principal: melhores ativos da última classificação.
--    `grupo_usado` e `n_grupo` mostram contra quem cada ativo foi comparado; `confianca`
--    resume quanto do score pôde de fato ser calculado.
SELECT
    ticker,
    nota,
    ROUND(score_final, 1) AS score,
    ROUND(score_valuation, 0) AS valuation,
    ROUND(score_rentabilidade, 0) AS rentabilidade,
    ROUND(score_solidez, 0) AS solidez,
    ROUND(score_dividendos, 0) AS dividendos,
    setor,
    porte,
    perfil_risco,
    perfil_investidor,
    grupo_usado,
    n_grupo,
    confianca,
    fonte
FROM hive.default.gold_classificacao_ativos
WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
ORDER BY score_final DESC NULLS LAST;

-- 3) Os 3 melhores de cada setor.
--    Comparar P/L de banco com P/L de mineradora não faz sentido; aqui o recorte é setorial.
SELECT setor, ticker, nota, ROUND(score_final, 1) AS score, perfil_investidor
FROM (
    SELECT
        setor,
        ticker,
        nota,
        score_final,
        perfil_investidor,
        ROW_NUMBER() OVER (PARTITION BY setor ORDER BY score_final DESC) AS posicao
    FROM hive.default.gold_classificacao_ativos
    WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
      AND score_final IS NOT NULL
)
WHERE posicao <= 3
ORDER BY setor, posicao;

-- 4) O screener no formato do Fundamentus: as 22 colunas originais, com os nomes do site,
--    mais a classificação que este projeto acrescenta.
SELECT * FROM hive.default.gold_screener_fundamentus
ORDER BY "Score" DESC NULLS LAST;

-- 5) Distribuição das notas. Os cortes são FIXOS (A >= 80, B >= 65, C >= 50, D >= 35),
--    não quintis — um universo inteiro de ativos ruins deve produzir vários E, e não
--    redistribuir as notas para dar 20% de A de qualquer jeito.
SELECT
    nota,
    COUNT(*) AS ativos,
    ROUND(MIN(score_final), 1) AS score_min,
    ROUND(MAX(score_final), 1) AS score_max
FROM hive.default.gold_classificacao_ativos
WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
GROUP BY nota
ORDER BY nota;

-- 6) DIAGNÓSTICO MAIS ÚTIL: cobertura de dados por indicador.
--    Mostra quais dos 22 indicadores a API realmente entrega. Esperado no plano free:
--    `margem_bruta` e `liq_corrente` nulos para as financeiras (bancos não têm lucro bruto
--    nem capital de giro no sentido industrial), e `preco_capital_giro`,
--    `preco_ativo_circ_liquido` e `cresc_receita_5a` nulos para todos.
SELECT
    COUNT(*) AS total,
    COUNT(preco_lucro) AS preco_lucro,
    COUNT(preco_valor_patrimonial) AS p_vp,
    COUNT(psr) AS psr,
    COUNT(dividend_yield) AS div_yield,
    COUNT(preco_ativo) AS p_ativo,
    COUNT(preco_capital_giro) AS p_cap_giro,
    COUNT(preco_ebit) AS p_ebit,
    COUNT(preco_ativo_circ_liquido) AS p_acl,
    COUNT(ev_ebit) AS ev_ebit,
    COUNT(ev_ebitda) AS ev_ebitda,
    COUNT(margem_bruta) AS mrg_bruta,
    COUNT(margem_ebit) AS mrg_ebit,
    COUNT(margem_liquida) AS mrg_liq,
    COUNT(liq_corrente) AS liq_corr,
    COUNT(roic) AS roic,
    COUNT(roe) AS roe,
    COUNT(liquidez_2meses) AS liq_2m,
    COUNT(patrimonio_liquido) AS patrim_liq,
    COUNT(divida_liquida_patrimonio) AS div_liq_patrim,
    COUNT(cresc_receita_5a) AS cresc_5a
FROM hive.default.silver_indicadores_fundamentalistas
WHERE data_coleta = (SELECT MAX(data_coleta) FROM hive.default.silver_indicadores_fundamentalistas);

-- 7) Procedência dos indicadores derivados: de onde cada número realmente veio.
--    Nenhum NULL neste pipeline é anônimo.
SELECT
    ticker,
    fonte,
    preco_ativo_origem,
    roic_origem,
    dividend_yield_origem,
    liquidez_2meses_origem,
    cresc_receita_origem,
    pontos_serie,
    ROUND(cobertura_campos_pct, 2) AS cobertura,
    motivo_inelegibilidade
FROM hive.default.silver_indicadores_fundamentalistas
WHERE data_coleta = (SELECT MAX(data_coleta) FROM hive.default.silver_indicadores_fundamentalistas)
ORDER BY cobertura_campos_pct DESC, ticker;

-- 8) Fórmula de Graham: ativos negociando abaixo do valor intrínseco sqrt(22.5 * LPA * VPA).
SELECT
    ticker,
    ROUND(cotacao, 2) AS cotacao,
    ROUND(valor_graham, 2) AS valor_graham,
    ROUND(margem_seguranca * 100, 1) AS margem_seguranca_pct,
    nota,
    fonte
FROM hive.default.gold_classificacao_ativos
WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
  AND flag_graham = true
ORDER BY margem_seguranca DESC;

-- 9) Método Bazin: preço-teto para um yield-alvo de 6% ao ano.
--    Só é calculado quando o dividend yield veio da própria API (o módulo de dividendos é
--    bloqueado no plano free, então um DY estimado produziria um preço-teto sem significado).
SELECT
    ticker,
    ROUND(cotacao, 2) AS cotacao,
    ROUND(preco_teto_bazin, 2) AS preco_teto,
    ROUND((preco_teto_bazin / cotacao - 1) * 100, 1) AS desconto_pct,
    nota,
    perfil_investidor
FROM hive.default.gold_classificacao_ativos
WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
  AND flag_bazin = true
ORDER BY desconto_pct DESC;

-- 10) Magic Formula de Greenblatt: soma do rank de EV/EBIT com o rank de ROIC.
--     Combina "barato" com "bom negócio". Menor soma = melhor.
SELECT
    g.ticker,
    g.rank_magic_formula,
    ROUND(i.ev_ebit, 2) AS ev_ebit,
    ROUND(i.roic * 100, 1) AS roic_pct,
    g.nota,
    g.fonte
FROM hive.default.gold_classificacao_ativos AS g
JOIN hive.default.silver_indicadores_fundamentalistas AS i
  ON i.ticker = g.ticker AND i.data_coleta = g.data_referencia
WHERE g.data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
  AND g.rank_magic_formula IS NOT NULL
ORDER BY g.rank_magic_formula
LIMIT 10;

-- 11) Screening clássico combinando as flags: barato, rentável e pouco endividado.
SELECT ticker, nota, ROUND(score_final, 1) AS score, setor, perfil_investidor
FROM hive.default.gold_classificacao_ativos
WHERE data_referencia = (SELECT MAX(data_referencia) FROM hive.default.gold_classificacao_ativos)
  AND flag_pl_barato = true
  AND flag_roe_bom = true
  AND flag_endiv_ok = true
ORDER BY score_final DESC NULLS LAST;

-- 12) Evolução do score entre execuções: o que a partição acumulativa da gold habilita.
--     Só retorna linhas a partir da segunda execução semanal.
SELECT
    ticker,
    data_referencia,
    ROUND(score_final, 1) AS score,
    nota,
    ROUND(score_final - LAG(score_final) OVER (PARTITION BY ticker ORDER BY data_referencia), 1) AS variacao
FROM hive.default.gold_classificacao_ativos
WHERE score_final IS NOT NULL
ORDER BY ticker, data_referencia DESC;

-- 13) TESTE DE IDEMPOTÊNCIA: dispare a DAG duas vezes e rode isto.
--     `linhas` tem que ser igual a `tickers` em toda partição. Se divergir, o overwrite
--     dinâmico de partição não está ativo e o pipeline está duplicando dados.
SELECT data_coleta, COUNT(*) AS linhas, COUNT(DISTINCT ticker) AS tickers
FROM hive.default.silver_indicadores_fundamentalistas
GROUP BY data_coleta
ORDER BY data_coleta DESC;

-- 14) Série histórica REAL vinda da brapi, lado a lado com a tabela sintética do pipeline de
--     preços. As duas existem de propósito e nunca são misturadas: `precos_acoes` é um random
--     walk gerado localmente, `silver_precos_brapi` são pregões de verdade.
SELECT 'brapi (real)' AS origem, ticker, COUNT(*) AS pregoes,
       MIN(data_pregao) AS de, MAX(data_pregao) AS ate
FROM hive.default.silver_precos_brapi
GROUP BY ticker
UNION ALL
SELECT 'producer (sintetico)' AS origem, ticker, COUNT(DISTINCT data_pregao) AS pregoes,
       MIN(data_pregao) AS de, MAX(data_pregao) AS ate
FROM hive.default.precos_acoes
GROUP BY ticker
ORDER BY origem, ticker;
