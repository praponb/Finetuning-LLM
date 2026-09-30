SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    pos,
    entry.qty,
    entry.promo
FROM carts
LATERAL VIEW POSEXPLODE(items) e AS pos, entry
