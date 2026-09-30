SELECT id, category
FROM products
WHERE category <> 'Fashion'
UNION ALL
SELECT id, category
FROM products
WHERE price < 310;
