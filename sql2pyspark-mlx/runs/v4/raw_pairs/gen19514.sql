WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS line
)
SELECT
    created_at,
    product_id,
    qty,
    price,
    promo,
    CASE WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'PRIORITY' WHEN promo RLIKE '^(SPRING10|WELCOME5)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS promo_zone
FROM exploded
ORDER BY product_id DESC, qty;
