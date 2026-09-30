SELECT MAX(launch_date) AS last_launch_date
FROM products
WHERE price BETWEEN 370 AND 550 AND price > 480
ORDER BY last_launch_date
