SELECT shipping_fee, 'order' AS origin
FROM orders
UNION
SELECT price AS shipping_fee, 'product' AS origin
FROM products
