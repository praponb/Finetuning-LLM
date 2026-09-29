SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    line.product_id,
    line.price
FROM carts
LATERAL VIEW OUTER EXPLODE(items) x AS line;
