SELECT
    e.id,
    e.manager_id,
    e.salary,
    e.hire_date
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering')
ORDER BY hire_date DESC, e.id
LIMIT 10
