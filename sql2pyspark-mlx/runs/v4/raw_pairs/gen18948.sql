SELECT dept_name, COUNT(CASE WHEN location = 'Singapore' THEN 1 END) singapore_count, ROUND((100 * SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_chiang_mai
FROM departments
WHERE (location = 'New York' AND location NOT IN ('New York', 'Singapore', 'Berlin')) OR location IN ('London', 'Tokyo', 'Chiang Mai')
GROUP BY dept_name
ORDER BY singapore_count DESC, dept_name ASC
LIMIT 3
