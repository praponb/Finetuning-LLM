SELECT
    d.dept_name,
    d.budget,
    d.location,
    e.full_name,
    e.hire_date
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id;
