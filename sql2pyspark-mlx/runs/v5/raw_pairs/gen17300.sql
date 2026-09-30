SELECT s.id, s.order_id
FROM shipments s
WHERE s.order_id IN (SELECT id FROM orders WHERE amount > 1540);
