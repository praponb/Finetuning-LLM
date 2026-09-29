SELECT
    id,
    customer_id,
    created_at,
    EXPLODE_OUTER(coupon_codes) AS element
FROM carts
