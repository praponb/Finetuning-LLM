SELECT id, supplier_id
FROM products
WHERE category = 'Fashion'
INTERSECT
SELECT id, supplier_id
FROM products
WHERE stock_qty <> 223;
