SELECT id, launch_date
FROM products
WHERE product_name IS NOT NULL
INTERSECT
SELECT id, launch_date
FROM products
WHERE supplier_id IS NULL
