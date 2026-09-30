SELECT status FROM orders UNION SELECT status FROM shipments WHERE order_id = 15 ORDER BY status;
