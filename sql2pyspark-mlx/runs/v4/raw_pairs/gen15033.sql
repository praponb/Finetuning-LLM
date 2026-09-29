SELECT level, SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary
FROM employees
WHERE full_name IS NOT NULL AND MONTH(hire_date) IN (3, 8, 12)
GROUP BY level
ORDER BY 1 DESC
LIMIT 10
