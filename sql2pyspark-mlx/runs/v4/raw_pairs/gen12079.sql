WITH top_sessions AS (SELECT id, browser, started_at, DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at) AS rn FROM sessions) SELECT id, browser, started_at FROM top_sessions WHERE rn <= 2
