SELECT SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count
FROM employees
ORDER BY l2_count DESC
