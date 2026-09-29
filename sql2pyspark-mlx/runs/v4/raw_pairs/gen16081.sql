SELECT
    id,
    customer_id,
    coupon_codes,
    pos,
    elem.product_id,
    elem.qty,
    elem.price
FROM carts
LATERAL VIEW POSEXPLODE(items) t AS pos, elem;
