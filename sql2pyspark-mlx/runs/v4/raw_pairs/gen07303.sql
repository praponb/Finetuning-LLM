SELECT ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4
FROM employees
ORDER BY pct_l4
