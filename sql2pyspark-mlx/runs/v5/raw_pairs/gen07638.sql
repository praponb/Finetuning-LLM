SELECT oi.id, oi.product_id FROM order_items oi WHERE oi.product_id = 30 AND oi.product_id NOT IN (SELECT id FROM products WHERE cost >= 470);
