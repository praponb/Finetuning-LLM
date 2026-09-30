SELECT s.id, s.delivered_date, s.cost, s.carrier FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE customer_id IS NULL)
