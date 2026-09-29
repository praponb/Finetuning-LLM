SELECT id, category, stock_qty
FROM products
WHERE price IS NOT NULL
UNION
SELECT id, category, stock_qty
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance');
