WITH exploded AS (
    SELECT
        t0.id,
        t0.created_at,
        t0.coupon_codes,
        elem.price,
        elem.promo
    FROM carts t0
    LATERAL VIEW EXPLODE(t0.items) t AS elem
),
flattened AS (
    SELECT
        id,
        promo,
        value
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
filtered AS (
    SELECT
        *,
        CONCAT(value, ', ', promo) AS value_promo,
        COALESCE(value, 'N/A') AS value_clean
    FROM flattened
    WHERE value IS NOT NULL
      AND value = 'BUNDLE'
),
valid AS (
    SELECT
        promo,
        value_promo,
        CASE WHEN value IN ('BUNDLE', 'VIP') THEN 'Priority' ELSE 'Other' END AS value_group,
        COALESCE(value, 'UNKNOWN') AS resolved_value
    FROM filtered
    WHERE value IS NOT NULL
)
SELECT
    promo,
    COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END) AS welcome5_count,
    COUNT(CASE WHEN promo = 'VIP' THEN 1 END) AS vip_count,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count,
    COUNT(*) FILTER (WHERE promo = 'SPRING10') AS spring10_rows
FROM valid
GROUP BY 1
ORDER BY bundle_count DESC;
