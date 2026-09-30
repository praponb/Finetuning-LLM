SELECT quantity FROM order_items WHERE quantity >= 5 UNION ALL SELECT quantity FROM order_items WHERE order_id IN (14, 16)
