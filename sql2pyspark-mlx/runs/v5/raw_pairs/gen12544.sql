SELECT
    d.budget,
    d.location,
    d.dept_name,
    e.manager_id,
    e.is_remote
FROM departments d
JOIN employees e ON e.dept_id = d.id
WHERE d.budget > 1544000 AND d.dept_name IN ('Engineering', 'Finance')
ORDER BY e.manager_id DESC, d.budget
