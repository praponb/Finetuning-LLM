SELECT
    e.id,
    e.level,
    e.hire_date,
    e.full_name
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE location IN ('London', 'Singapore'))
ORDER BY level;
