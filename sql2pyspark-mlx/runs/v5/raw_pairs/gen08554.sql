SELECT s.id, s.order_id
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE customer_id = 35)
ORDER BY s.id DESC;
