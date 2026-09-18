-- ============================================================
-- Consultas de exemplo sobre a tabela hive.default.precos_acoes
-- Execute via Trino (equivalente local ao AWS Athena).
--
-- Como rodar (veja detalhes no README):
--   docker exec -it trino trino --catalog hive --schema default
-- e cole cada bloco, ou rode o arquivo inteiro com --file.
-- ============================================================

-- 1) Sanity check: a tabela existe e tem dados?
SELECT *
FROM hive.default.precos_acoes
LIMIT 10;

-- 2) Quantos eventos (linhas) temos por dia de pregão?
--    Útil para confirmar que as partições foram registradas certinho.
SELECT data_pregao, COUNT(*) AS qtd_eventos
FROM hive.default.precos_acoes
GROUP BY data_pregao
ORDER BY data_pregao DESC;

-- 3) Último preço de fechamento registrado por ticker
--    (janela ROW_NUMBER pra pegar o evento mais recente de cada ação)
SELECT ticker, data_pregao, fechamento, evento_ts
FROM (
    SELECT
        ticker,
        data_pregao,
        fechamento,
        evento_ts,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY evento_ts DESC) AS rn
    FROM hive.default.precos_acoes
)
WHERE rn = 1
ORDER BY ticker;

-- 4) Variação percentual do dia (fechamento vs abertura) por evento,
--    identificando as maiores altas e quedas.
SELECT
    ticker,
    data_pregao,
    abertura,
    fechamento,
    ROUND(((fechamento - abertura) / abertura) * 100, 2) AS variacao_pct
FROM hive.default.precos_acoes
ORDER BY variacao_pct DESC
LIMIT 10;

-- 5) Preço médio, máximo e mínimo por ticker no período todo
SELECT
    ticker,
    ROUND(AVG(fechamento), 2) AS preco_medio,
    ROUND(MAX(maxima), 2) AS maxima_periodo,
    ROUND(MIN(minima), 2) AS minima_periodo,
    SUM(volume) AS volume_total
FROM hive.default.precos_acoes
GROUP BY ticker
ORDER BY volume_total DESC;

-- 6) Dia de maior volume negociado, por ticker
SELECT ticker, data_pregao, volume
FROM (
    SELECT
        ticker,
        data_pregao,
        volume,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY volume DESC) AS rn
    FROM hive.default.precos_acoes
)
WHERE rn = 1
ORDER BY volume DESC;
