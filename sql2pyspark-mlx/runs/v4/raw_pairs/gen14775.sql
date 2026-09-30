SELECT
    d.location,
    e.is_remote,
    e.dept_id,
    e.salary
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.manager_id IS NULL
