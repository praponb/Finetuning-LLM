WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        pos + 1 AS line_no,
        elem.qty,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) t AS pos, elem
)
SELECT
    id,
    coupon_codes,
    line_no,
    qty,
    promo,
    CASE WHEN promo RLIKE '^(SPRING10|WELCOME5)$' THEN 'GROUP_A' WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'CORE' ELSE 'GROUP_B' END AS promo_zone
FROM exploded
ORDER BY promo_zone DESC;
