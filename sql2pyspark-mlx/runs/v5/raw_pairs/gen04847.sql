SELECT
    id,
    started_at,
    LAST_VALUE(country, TRUE) OVER w_hist AS last_known_country,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS event_seq,
    FIRST_VALUE(browser) IGNORE NULLS OVER w_hist_2 AS first_known_browser
FROM sessions
WHERE device IN ('mobile', 'tablet')
WINDOW w_hist AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
