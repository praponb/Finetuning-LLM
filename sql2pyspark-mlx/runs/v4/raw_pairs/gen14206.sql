WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER w_running AS seq_no_2
    FROM sessions
    WINDOW w_running AS (PARTITION BY browser, device ORDER BY started_at DESC, id)
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    event_seq,
    seq_no,
    row_seq
FROM metrics
ORDER BY started_at DESC
