SELECT
    id,
    customer_id,
    coupon_codes,
    key,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) t AS key, v
