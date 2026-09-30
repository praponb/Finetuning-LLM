SELECT category, COUNT(*) AS num_products, MAX(launch_date) AS last_launch_date
FROM products
WHERE cost > 230
GROUP BY category
HAVING COUNT(*) >= 2
