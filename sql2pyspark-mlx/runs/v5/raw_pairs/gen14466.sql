SELECT tags
FROM products
WHERE MONTH(launch_date) IN (4, 5, 12);
