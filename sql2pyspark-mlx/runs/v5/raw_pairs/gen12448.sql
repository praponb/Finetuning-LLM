SELECT cost AS value, 'product' AS kind FROM products UNION ALL SELECT unit_price AS value, 'order_item' AS kind FROM order_items
