SELECT category
FROM products
INTERSECT ALL
SELECT category
FROM products
WHERE LOWER(TRIM(product_name)) = 'smart watch';
