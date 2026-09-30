SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id ASC) seq_no,
    ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY started_at DESC, id ASC) seq_no_2,
    ROW_NUMBER() OVER (PARTITION BY customer_id, country ORDER BY started_at ASC, id ASC) row_seq,
    ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY started_at DESC, id ASC) row_seq_2,
    ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at ASC, id ASC) event_seq
FROM sessions
