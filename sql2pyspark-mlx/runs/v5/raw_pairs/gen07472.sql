SELECT 'product' AS source, cost FROM products UNION SELECT 'order' AS source, shipping_fee FROM orders;
