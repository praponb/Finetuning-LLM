SELECT oi.id, oi.product_id, oi.order_id
FROM order_items oi
WHERE oi.quantity BETWEEN 3 AND 5 AND oi.product_id NOT IN (SELECT id FROM products WHERE price <= 440)
