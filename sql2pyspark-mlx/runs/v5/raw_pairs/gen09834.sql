SELECT
    id,
    customer_id,
    browser,
    country,
    ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS event_seq,
    ROW_NUMBER() OVER w_recent AS seq_no,
    FIRST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
    ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at, id) AS seq_no_2
FROM sessions
WHERE country IN ('DE', 'SG', 'UK')
WINDOW w_recent AS (PARTITION BY customer_id, country ORDER BY started_at DESC, id)
