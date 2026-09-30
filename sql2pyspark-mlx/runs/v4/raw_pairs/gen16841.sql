SELECT
    id,
    customer_id,
    created_at,
    pos,
    item.qty,
    item.price
FROM carts
LATERAL VIEW POSEXPLODE(items) t AS pos, item;
