SELECT
    id,
    browser,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS event_seq,
    LAST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS event_seq_2
FROM sessions
WHERE TO_DATE(started_at) < '2023-10-10';
