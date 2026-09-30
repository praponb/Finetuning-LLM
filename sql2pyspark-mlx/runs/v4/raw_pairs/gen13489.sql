SELECT quantity FROM order_items WHERE product_id IN (3, 32, 38) UNION ALL SELECT quantity FROM order_items WHERE order_id IN (10, 19) ORDER BY quantity
