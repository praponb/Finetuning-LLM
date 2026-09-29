SELECT 'order_item' kind, unit_price FROM order_items UNION SELECT 'product' kind, price unit_price FROM products WHERE SIZE(tags) > 1 ORDER BY kind
