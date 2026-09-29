SELECT
    *,
    FIRST_VALUE(country) IGNORE NULLS OVER w_running AS first_known_country,
    ROW_NUMBER() OVER (ORDER BY started_at, id) AS event_seq,
    LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser
FROM sessions
WINDOW w_running AS (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
