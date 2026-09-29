SELECT d.location, e.salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id;
