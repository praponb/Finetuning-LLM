WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
)
SELECT
    CASE WHEN price >= 188 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count,
    ROUND(SUM(qty) / NULLIF(COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END), 0), 2) AS qty_per_welcome5
FROM flattened
GROUP BY CASE WHEN price >= 188 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY price_tier
