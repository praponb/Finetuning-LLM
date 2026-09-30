SELECT id, product_name
FROM products
WHERE price >= 630
UNION ALL
SELECT id, product_name
FROM products
WHERE supplier_id IN (15, 28, 38, 39);
