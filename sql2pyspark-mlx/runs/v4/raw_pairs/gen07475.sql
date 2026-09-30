SELECT
    e.id,
    e.level,
    e.manager_id,
    e.email,
    d.location,
    d.dept_name,
    d.budget
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
ORDER BY email, id DESC;
