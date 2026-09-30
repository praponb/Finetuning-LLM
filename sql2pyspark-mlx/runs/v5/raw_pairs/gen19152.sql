SELECT d.dept_name, e.level
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
ORDER BY dept_name ASC
