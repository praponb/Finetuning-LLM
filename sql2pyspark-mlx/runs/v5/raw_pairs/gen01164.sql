SELECT
    id,
    customer_id,
    created_at,
    rec.qty,
    rec.price,
    rec.promo
FROM carts
LATERAL VIEW EXPLODE(items) x AS rec;
