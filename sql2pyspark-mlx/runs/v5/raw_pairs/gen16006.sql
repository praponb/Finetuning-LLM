SELECT s.id, s.status
FROM shipments s
WHERE s.order_id IS NULL AND s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee >= 30)
ORDER BY s.status DESC;
