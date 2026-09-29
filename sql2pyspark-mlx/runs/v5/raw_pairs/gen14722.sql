SELECT
    s.id,
    s.status,
    s.order_id,
    s.cost
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status = 'PENDING')
ORDER BY 3, s.cost DESC
