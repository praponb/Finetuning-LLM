SELECT cost, 'product' AS origin
FROM products
UNION
SELECT amount AS cost, 'order' AS origin
FROM orders
WHERE order_date > '2024-01-05'
