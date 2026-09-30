SELECT
    e.*,
    d.location,
    d.dept_name,
    d.budget
FROM employees e
LEFT JOIN departments d
    ON e.dept_id = d.id;
