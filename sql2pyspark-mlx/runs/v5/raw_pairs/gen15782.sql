SELECT
    src.id,
    src.customer_id,
    src.created_at,
    src.coupon_codes,
    pos + 1 AS position,
    rec.product_id,
    rec.price
FROM carts src
LATERAL VIEW OUTER POSEXPLODE(src.items) t AS pos, rec;
