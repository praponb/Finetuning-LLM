WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.price,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) e
)
SELECT
    id,
    created_at,
    coupon_codes,
    price
FROM flattened
ORDER BY price DESC
