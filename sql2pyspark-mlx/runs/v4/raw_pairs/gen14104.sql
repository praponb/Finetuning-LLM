SELECT id, stock_qty, YEAR(launch_date) AS launch_date_year
FROM products
WHERE price <= 640
