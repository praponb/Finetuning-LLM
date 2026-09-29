SELECT price AS value, 'product' AS record_type
FROM products
WHERE product_name IS NOT NULL
UNION
SELECT amount AS value, 'order' AS record_type
FROM orders
WHERE discount > 0.37;
