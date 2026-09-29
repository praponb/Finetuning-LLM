WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) x AS pos, elem
)
SELECT
    id,
    created_at,
    pos,
    product_id,
    qty,
    price,
    promo,
    CASE WHEN promo RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_A' WHEN promo RLIKE '^(SPRING10|BUNDLE)$' THEN 'CORE' ELSE 'GROUP_B' END AS promo_zone
FROM flattened
