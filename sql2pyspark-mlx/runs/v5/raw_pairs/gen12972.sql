SELECT s.id, s.delivered_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE customer_id IS NULL);
