WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        pos,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, elem
)
-- Multi-level aggregation
SELECT
    CASE WHEN price >= 210 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows
FROM lines
GROUP BY CASE WHEN price >= 210 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', created_at)
ORDER BY hour_window DESC NULLS FIRST;
