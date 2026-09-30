SELECT COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count, MIN(launch_date) AS min_launch_date
FROM products
