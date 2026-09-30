SELECT
    e.id,
    e.level,
    e.hire_date,
    e.manager_id
FROM employees e
WHERE MONTH(e.hire_date) IN (1, 3, 4) AND e.dept_id NOT IN (SELECT id FROM departments WHERE budget <= 2122000)
ORDER BY hire_date DESC
