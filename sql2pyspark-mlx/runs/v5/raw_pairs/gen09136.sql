SELECT launch_date, 'product' AS source
FROM products
UNION ALL
SELECT sale_date, 'store_sale' AS source
FROM store_sales
WHERE revenue BETWEEN 2900 AND 3500
