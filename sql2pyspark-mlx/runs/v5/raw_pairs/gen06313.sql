WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t0.id,
        t0.customer_id,
        t0.created_at,
        t0.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts t0
    LATERAL VIEW OUTER EXPLODE(t0.items) t AS li_item
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END AS qty_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT(*) AS row_count,
    MAX(created_at) AS last_seen,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    PERCENTILE_APPROX(product_id, ARRAY(0.25, 0.5, 0.75)) AS product_id_quartiles,
    COUNT(DISTINCT promo) AS distinct_promos
FROM exploded
GROUP BY DATE_TRUNC('HOUR', created_at), CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', created_at)
