WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
)
SELECT
    id,
    customer_id,
    created_at
FROM lines
ORDER BY customer_id NULLS FIRST, created_at;
