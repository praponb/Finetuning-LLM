SELECT oi.id, oi.product_id FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE price IS NOT NULL)
