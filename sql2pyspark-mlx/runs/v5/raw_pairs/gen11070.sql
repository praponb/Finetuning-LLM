SELECT sale_date
FROM store_sales
UNION ALL
SELECT launch_date AS sale_date
FROM products
ORDER BY sale_date DESC
