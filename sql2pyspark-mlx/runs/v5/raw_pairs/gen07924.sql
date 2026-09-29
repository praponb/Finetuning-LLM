SELECT oi.id, oi.order_id, oi.unit_price FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE category NOT IN ('Sports', 'Home', 'Books'))
