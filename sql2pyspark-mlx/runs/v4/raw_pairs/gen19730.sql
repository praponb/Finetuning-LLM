WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        line.product_id,
        line.qty
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS line
)
-- Grouped metrics
SELECT
    CASE WHEN qty >= 5 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_tier,
    AVG(qty) AS avg_qty,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(DISTINCT id) AS distinct_ids
FROM lines
GROUP BY CASE WHEN qty >= 5 THEN 'PREMIUM' ELSE 'STANDARD' END
