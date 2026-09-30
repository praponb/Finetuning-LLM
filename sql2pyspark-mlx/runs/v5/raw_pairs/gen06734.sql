SELECT
    t.id,
    t.coupon_codes,
    pos,
    elem.product_id,
    elem.qty,
    elem.promo
FROM carts t
LATERAL VIEW OUTER POSEXPLODE(t.items) x AS pos, elem
