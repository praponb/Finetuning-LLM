SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count, ROUND((100 * SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_electronics
FROM products
GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM');
