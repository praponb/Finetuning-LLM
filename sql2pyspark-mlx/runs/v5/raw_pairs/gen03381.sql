SELECT d.location, e.is_remote
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.is_remote;
