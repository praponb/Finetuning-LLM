SELECT id, full_name, level
FROM employees
WHERE dept_id IN (14, 33, 35, 36)
UNION ALL
SELECT id, full_name, level
FROM employees
WHERE YEAR(hire_date) = 2025
ORDER BY level;
