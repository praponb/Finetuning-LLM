SELECT id, browser, started_at, PERCENT_RANK() OVER (PARTITION BY browser ORDER BY started_at) started_at_pct_rank FROM sessions;
