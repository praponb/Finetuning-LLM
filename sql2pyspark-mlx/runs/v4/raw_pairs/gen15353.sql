SELECT
    id,
    customer_id,
    coupon_codes,
    key,
    value
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) li AS key, value;
