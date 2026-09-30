SELECT category, DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, COUNT(*) AS num_products
FROM products
WHERE category NOT IN ('Books', 'Grocery')
GROUP BY category, DATE_FORMAT(launch_date, 'yyyy-MM')
HAVING COUNT(*) >= 3;
