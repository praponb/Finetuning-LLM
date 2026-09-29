SELECT e.id, e.full_name, e.salary
FROM employees e
WHERE UPPER(e.email) LIKE '%IAM%' AND e.dept_id NOT IN (SELECT id FROM departments WHERE location IN ('New York', 'Singapore', 'London'))
ORDER BY full_name;
