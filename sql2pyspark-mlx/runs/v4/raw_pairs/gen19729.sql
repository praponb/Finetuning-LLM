SELECT id, launch_date, stock_qty
FROM products
WHERE product_name LIKE '%Bottle%'
UNION ALL
SELECT id, launch_date, stock_qty
FROM products
WHERE product_name LIKE '%USB%'
