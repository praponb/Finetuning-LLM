SELECT product_id FROM order_items WHERE quantity <> 4 UNION ALL SELECT product_id FROM order_items WHERE order_id IN (24, 35, 37, 39) ORDER BY product_id;
