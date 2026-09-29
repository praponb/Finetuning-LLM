WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, elem
)
-- Multi-level aggregation
SELECT
    promo,
    MIN(created_at) AS first_seen,
    COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END) AS welcome5_count
FROM flattened
GROUP BY promo
ORDER BY promo DESC NULLS FIRST;
