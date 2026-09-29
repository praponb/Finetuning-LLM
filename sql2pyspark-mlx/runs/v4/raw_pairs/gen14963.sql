SELECT o.id, o.order_date, o.customer_id FROM orders o WHERE o.status = 'COMPLETED' AND EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id) ORDER BY o.customer_id
