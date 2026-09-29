SELECT s.id, s.shipped_date, s.status, s.order_id FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status IN ('SHIPPED', 'CANCELLED'));
