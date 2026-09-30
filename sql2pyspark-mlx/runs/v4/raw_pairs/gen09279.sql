SELECT id, order_id, product_id FROM order_items WHERE order_id IN (14, 15, 16) UNION ALL SELECT id, order_id, product_id FROM order_items WHERE quantity BETWEEN 3 AND 6 ORDER BY product_id
