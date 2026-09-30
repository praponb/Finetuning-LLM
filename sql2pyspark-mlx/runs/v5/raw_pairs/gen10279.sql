SELECT
    o.id,
    o.store_id,
    o.discount,
    o.status
FROM orders o
WHERE o.id IN (SELECT order_id FROM shipments)
ORDER BY status, id
LIMIT 1
