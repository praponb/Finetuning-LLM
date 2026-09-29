SELECT amount
FROM orders
WHERE amount IS NOT NULL
UNION
SELECT cost amount
FROM products;
