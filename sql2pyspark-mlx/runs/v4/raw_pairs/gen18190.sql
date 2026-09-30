WITH lines AS (
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
    LATERAL VIEW EXPLODE(items) e AS line
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    product_id,
    ROUND(SUM(qty) / NULLIF(COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END), 0), 2) AS qty_per_bundle,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    COUNT(*) FILTER (WHERE promo = 'WELCOME5') AS welcome5_rows,
    GROUPING(DATE_TRUNC('HOUR', created_at)) AS grouping_level
FROM lines
GROUP BY CUBE (DATE_TRUNC('HOUR', created_at), product_id)
HAVING COUNT(*) FILTER (WHERE promo = 'WELCOME5') >= 1
ORDER BY bundle_rows;
