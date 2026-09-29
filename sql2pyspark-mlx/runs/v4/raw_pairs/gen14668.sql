WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    CASE WHEN price >= 211 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    CASE WHEN qty >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_tier,
    COUNT(*) FILTER (WHERE promo = 'WELCOME5') AS welcome5_rows,
    PERCENTILE_APPROX(product_id, 0.5) AS product_id_p50,
    SUM(price) FILTER (WHERE promo = 'BUNDLE') AS bundle_total_price,
    PERCENTILE_APPROX(product_id, 0.5) AS product_id_p50_2,
    COUNT(*) AS cnt,
    RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', created_at) ORDER BY SUM(price) FILTER (WHERE promo = 'BUNDLE') DESC) AS bundle_total_price_rank
FROM flattened
GROUP BY DATE_TRUNC('HOUR', created_at), CASE WHEN price >= 211 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY welcome5_rows DESC NULLS LAST
