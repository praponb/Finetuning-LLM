SELECT
    d.location,
    d.dept_name,
    d.budget,
    e.manager_id,
    e.hire_date
FROM departments d
INNER JOIN employees e ON e.dept_id = d.id
WHERE e.dept_id IN (6, 8, 17)
ORDER BY d.dept_name;
