SELECT launch_date AS event_date FROM products EXCEPT SELECT delivered_date AS event_date FROM shipments WHERE order_id IS NOT NULL
