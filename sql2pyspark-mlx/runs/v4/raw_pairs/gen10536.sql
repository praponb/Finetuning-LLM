SELECT s.id, s.shipped_date, s.status, s.delivered_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE amount >= 600);
