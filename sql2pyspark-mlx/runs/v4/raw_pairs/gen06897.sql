SELECT id, browser, started_at, RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) AS started_at_rank FROM sessions WHERE country = 'DE';
