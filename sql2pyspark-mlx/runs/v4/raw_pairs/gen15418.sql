SELECT product_id FROM order_items EXCEPT SELECT product_id FROM order_items WHERE order_id IN (19, 31, 35, 40);
