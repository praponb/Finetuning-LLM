SELECT s.id, s.shipped_date, s.status FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE store_id IS NOT NULL);
