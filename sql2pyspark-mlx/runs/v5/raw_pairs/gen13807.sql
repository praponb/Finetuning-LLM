SELECT product_id FROM order_items EXCEPT DISTINCT SELECT product_id FROM order_items WHERE quantity >= 5;
