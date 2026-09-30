SELECT
    id,
    created_at,
    value
FROM carts
LATERAL VIEW EXPLODE(coupon_codes) t AS value
