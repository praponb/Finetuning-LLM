SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attr_key,
    v
FROM carts
LATERAL VIEW EXPLODE(attributes) li AS attr_key, v;
