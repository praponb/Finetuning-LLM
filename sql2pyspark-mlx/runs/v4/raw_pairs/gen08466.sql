SELECT id, order_id FROM order_items WHERE product_id IS NOT NULL UNION ALL SELECT id, order_id FROM order_items WHERE quantity >= 6 ORDER BY id
