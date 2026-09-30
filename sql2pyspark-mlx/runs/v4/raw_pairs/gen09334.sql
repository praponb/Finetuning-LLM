WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS event_seq
    FROM sessions
)
SELECT
    id,
    customer_id,
    started_at,
    country,
    event_seq,
    CASE WHEN country RLIKE '^(JP|DE)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|US)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS country_zone
FROM scored
