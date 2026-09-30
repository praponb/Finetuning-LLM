SELECT
    e.salary,
    e.level,
    d.location,
    d.budget
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
ORDER BY level;
