SELECT s.id, s.cost FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE amount > 1370) ORDER BY cost DESC
