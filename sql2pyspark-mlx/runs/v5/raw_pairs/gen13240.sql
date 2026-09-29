WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS line
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    PERCENTILE_APPROX(price, 0.95) AS price_p95,
    APPROX_PERCENTILE(qty, 0.9) AS qty_p90,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS ended_at,
    COUNT(*) AS row_count
FROM flattened
GROUP BY DATE_TRUNC('DAY', created_at), CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY price_p95 NULLS FIRST
