SELECT
    *,
    ROW_NUMBER() OVER w_part AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY started_at, id) AS row_seq
FROM sessions
WINDOW w_part AS (PARTITION BY browser ORDER BY started_at, id);
