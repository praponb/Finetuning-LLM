SELECT
    id,
    customer_id,
    EXPLODE_OUTER(coupon_codes) AS element
FROM carts
