SELECT order_id, product_id FROM order_items INTERSECT ALL SELECT order_id, product_id FROM order_items WHERE quantity >= 8;
