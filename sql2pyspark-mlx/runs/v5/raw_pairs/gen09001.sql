SELECT shipping_fee AS value, 'order' AS kind FROM orders UNION SELECT price AS value, 'product' AS kind FROM products WHERE stock_qty < 280
