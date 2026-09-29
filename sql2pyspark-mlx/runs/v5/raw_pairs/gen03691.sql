SELECT manager_id, MAX(salary) - MIN(salary) AS salary_range, SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count
FROM employees
GROUP BY manager_id
