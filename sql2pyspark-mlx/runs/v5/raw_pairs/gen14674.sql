SELECT
    d.location,
    d.dept_name,
    e.full_name,
    e.is_remote,
    e.hire_date
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
ORDER BY d.location, d.dept_name
