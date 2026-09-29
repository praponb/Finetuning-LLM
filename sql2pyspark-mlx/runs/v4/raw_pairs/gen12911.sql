WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN product_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS product_id_tier,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    COUNT(*) FILTER (WHERE promo = 'SPRING10') AS spring10_rows,
    MIN(qty) AS min_qty,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(product_id), 2) AS avg_product_id
FROM exploded
GROUP BY CASE WHEN product_id >= 9 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_product_id >= 0
ORDER BY product_id_tier
