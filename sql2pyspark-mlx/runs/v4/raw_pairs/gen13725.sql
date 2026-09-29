SELECT
    d.location,
    d.budget,
    e.level,
    e.manager_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
ORDER BY location;
