SELECT category, MAX(launch_date) AS last_launch_date, ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grocery FROM products GROUP BY category
