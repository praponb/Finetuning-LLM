SELECT category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, ROUND((100 * SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home
FROM products
WHERE launch_date > '2025-05-01'
GROUP BY 1
ORDER BY 1 DESC, pct_home;
