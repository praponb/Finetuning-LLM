SELECT
    e.is_remote,
    d.location,
    d.budget,
    d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.id
ORDER BY d.location ASC, d.budget ASC;
