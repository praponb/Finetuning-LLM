SELECT
    id,
    browser,
    ROW_NUMBER() OVER (ORDER BY started_at, id) AS seq_no,
    LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at DESC, id) AS seq_no_2
FROM sessions;
