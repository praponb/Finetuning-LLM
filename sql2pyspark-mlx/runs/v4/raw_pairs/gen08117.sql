SELECT
    id,
    browser,
    country,
    started_at,
    PERCENT_RANK() OVER (PARTITION BY browser, country ORDER BY started_at DESC) started_at_pct_rank
FROM sessions;
