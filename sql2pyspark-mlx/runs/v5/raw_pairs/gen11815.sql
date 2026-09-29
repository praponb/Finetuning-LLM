WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) e
)
SELECT
    id,
    coupon_codes,
    product_id,
    qty,
    price,
    promo,
    CASE WHEN promo RLIKE '^(VIP|SPRING10)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS promo_zone
FROM lines
ORDER BY promo_zone ASC;
