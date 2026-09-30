SELECT id, shipped_date, order_id FROM shipments WHERE shipped_date > (SELECT MAX(shipped_date) FROM shipments) ORDER BY order_id;
