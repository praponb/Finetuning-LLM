SELECT s.id, s.cost
FROM shipments s
WHERE s.order_id IN (SELECT id FROM orders WHERE status IN ('PENDING', 'SHIPPED'));
