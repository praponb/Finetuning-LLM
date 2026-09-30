SELECT unit_price FROM order_items WHERE order_id IN (14, 24) UNION SELECT cost AS unit_price FROM shipments WHERE order_id IS NULL
