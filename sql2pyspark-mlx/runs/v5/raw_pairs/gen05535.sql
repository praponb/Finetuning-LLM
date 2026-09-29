SELECT order_id FROM order_items WHERE product_id IN (2, 14, 32, 37) UNION ALL SELECT order_id FROM order_items WHERE order_id IN (15, 25, 28);
