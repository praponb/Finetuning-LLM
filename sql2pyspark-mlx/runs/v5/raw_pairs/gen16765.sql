SELECT amount AS value
FROM orders
UNION
SELECT cost AS value
FROM products
WHERE supplier_id = 34
ORDER BY value
