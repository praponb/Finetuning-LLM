SELECT category, CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort, ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_grocery
FROM products
WHERE cost < 490
GROUP BY category, CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY 1;
