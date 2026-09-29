WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS line
)
SELECT
    id,
    created_at,
    qty,
    price
FROM lines
ORDER BY price DESC;
