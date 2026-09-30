WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num
        FROM payments
    ) ranked
    WHERE row_num = 1
)
-- Multi-level aggregation
SELECT
    order_id,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(DISTINCT order_id) AS distinct_order_ids_2,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    STDDEV_SAMP(amount) AS amount_stddev,
    MAX(paid_at) AS last_seen
FROM ranked
GROUP BY order_id
ORDER BY span_minutes;
