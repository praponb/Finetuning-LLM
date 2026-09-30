SELECT
    id,
    created_at,
    coupon_codes,
    line.product_id,
    line.promo
FROM carts
LATERAL VIEW EXPLODE(items) x AS line;
