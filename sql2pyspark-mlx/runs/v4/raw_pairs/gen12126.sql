SELECT cost
FROM products
WHERE product_name IS NOT NULL
UNION
SELECT amount AS cost
FROM orders
WHERE discount < 0.3
