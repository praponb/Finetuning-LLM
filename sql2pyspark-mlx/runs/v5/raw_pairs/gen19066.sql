SELECT s.id, s.delivered_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE order_date < '2024-01-01')
