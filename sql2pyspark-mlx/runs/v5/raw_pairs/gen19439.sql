SELECT
    id,
    created_at,
    coupon_codes,
    attr_key,
    value
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value;
