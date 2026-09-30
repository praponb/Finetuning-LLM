SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    k,
    attr_value
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
