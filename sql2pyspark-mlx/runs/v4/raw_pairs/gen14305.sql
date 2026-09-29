SELECT stock_qty, brand
FROM products
WHERE price < 600
UNION ALL
SELECT stock_qty, brand
FROM products
WHERE ARRAY_CONTAINS(tags, 'new')
ORDER BY brand DESC;
