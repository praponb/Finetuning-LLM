SELECT
    d.budget,
    d.dept_name,
    e.level,
    e.full_name
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id;
