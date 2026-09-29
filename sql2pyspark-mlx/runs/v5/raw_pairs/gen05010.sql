SELECT amount AS value
FROM orders
UNION
SELECT cost AS value
FROM products
ORDER BY value
