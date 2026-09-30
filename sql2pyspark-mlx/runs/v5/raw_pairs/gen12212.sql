SELECT s.id, s.carrier FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status = 'RETURNED')
