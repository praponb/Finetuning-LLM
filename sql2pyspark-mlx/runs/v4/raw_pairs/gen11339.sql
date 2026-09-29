SELECT
    id,
    created_at,
    coupon_codes,
    key,
    v
FROM carts
LATERAL VIEW EXPLODE(attributes) e AS key, v
