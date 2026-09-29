SELECT shipped_date FROM shipments WHERE order_id IN (12, 18, 37, 38) UNION ALL SELECT launch_date FROM products;
