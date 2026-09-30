SELECT
    o.id,
    o.store_id,
    o.amount,
    o.channel
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
