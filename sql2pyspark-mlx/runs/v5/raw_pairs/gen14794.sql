WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, attr_value
)
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attr_key,
    attr_value
FROM exploded
