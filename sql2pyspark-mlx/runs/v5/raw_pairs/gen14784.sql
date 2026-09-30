SELECT
    base.id,
    base.customer_id,
    base.created_at,
    pos,
    entry.product_id,
    entry.price,
    entry.promo
FROM carts base
LATERAL VIEW OUTER POSEXPLODE(base.items) t AS pos, entry
