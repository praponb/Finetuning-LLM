SELECT
    id,
    customer_id,
    device,
    browser,
    country,
    FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
    ROW_NUMBER() OVER w_hist seq_no,
    LAST_VALUE(browser, TRUE) OVER w_grp last_known_browser,
    ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at, id) event_seq,
    LAST_VALUE(country) IGNORE NULLS OVER w_grp last_known_country
FROM sessions
WINDOW w_hist AS (PARTITION BY country, customer_id ORDER BY started_at DESC, id), w_grp AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
