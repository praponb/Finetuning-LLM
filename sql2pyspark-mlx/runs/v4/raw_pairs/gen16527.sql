SELECT s.id, s.status, s.cost
FROM shipments s
WHERE s.order_id IN (SELECT id FROM orders WHERE channel NOT IN ('mobile', 'web'));
