SELECT category, MAX(launch_date) AS last_launch_date FROM products GROUP BY category HAVING COUNT(CASE WHEN category = 'Electronics' THEN 1 END) < 2;
