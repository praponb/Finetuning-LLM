SELECT
    d.budget,
    d.dept_name,
    d.location,
    e.email
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE d.dept_name != 'Marketing' OR e.level = 'L4'
ORDER BY e.email;
