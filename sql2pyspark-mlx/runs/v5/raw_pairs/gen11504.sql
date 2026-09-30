SELECT d.location, COUNT(e.id) AS employee_count, ROUND((100 * SUM(CASE WHEN e.level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE d.location = 'New York'
GROUP BY d.location
ORDER BY d.location
LIMIT 50;
