SELECT
    *,
    LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS event_seq,
    LAST_VALUE(country) IGNORE NULLS OVER w_grp AS last_known_country,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS row_seq,
    FIRST_VALUE(country, TRUE) OVER w_hist AS first_known_country
FROM sessions
WINDOW w_grp AS (PARTITION BY device, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY country, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
