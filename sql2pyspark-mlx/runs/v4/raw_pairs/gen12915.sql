SELECT
    d.location,
    d.budget,
    d.dept_name,
    e.email,
    e.hire_date,
    e.manager_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE d.dept_name NOT IN ('Marketing', 'Finance');
