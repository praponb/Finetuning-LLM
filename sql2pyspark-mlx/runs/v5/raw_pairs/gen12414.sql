SELECT oi.id, oi.unit_price, oi.order_id, oi.quantity FROM order_items oi WHERE oi.order_id IN (3, 22, 28, 40) AND oi.product_id IN (SELECT id FROM products WHERE SIZE(tags) >= 2)
