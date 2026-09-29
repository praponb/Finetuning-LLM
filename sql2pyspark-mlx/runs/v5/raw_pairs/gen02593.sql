SELECT e.id, e.dept_id
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE location IN ('New York', 'London', 'Chiang Mai'));
