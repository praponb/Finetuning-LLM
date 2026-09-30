SELECT
    id,
    coupon_codes,
    key,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
