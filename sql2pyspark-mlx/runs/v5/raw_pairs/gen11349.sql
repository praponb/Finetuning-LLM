SELECT 'order' AS origin, amount AS value
FROM orders
UNION ALL
SELECT 'product' AS origin, price AS value
FROM products
ORDER BY value;
