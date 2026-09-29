SELECT oi.id, oi.order_id, oi.product_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE brand IS NOT NULL) ORDER BY id;
