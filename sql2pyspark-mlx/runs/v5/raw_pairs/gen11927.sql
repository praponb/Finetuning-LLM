SELECT
    p.id,
    p.paid_at,
    p.status,
    p.order_id
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE store_id = 9)
ORDER BY p.order_id DESC, id
LIMIT 10
