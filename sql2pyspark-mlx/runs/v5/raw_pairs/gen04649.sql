SELECT s.id, s.order_id, s.shipped_date, s.carrier FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE status = 'RETURNED') ORDER BY 2;
