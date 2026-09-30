SELECT
    e.id,
    e.is_remote,
    e.salary,
    e.hire_date
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Legal')
