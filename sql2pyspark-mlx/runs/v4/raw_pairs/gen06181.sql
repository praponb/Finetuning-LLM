SELECT id, order_id FROM order_items WHERE product_id IN (14, 18, 28) UNION SELECT id, order_id FROM order_items WHERE product_id IS NULL
