SELECT id, product_id, order_id FROM order_items WHERE product_id IS NOT NULL UNION ALL SELECT id, product_id, order_id FROM order_items WHERE order_id IN (12, 18, 31, 34) ORDER BY product_id
