SELECT d.budget, e.level
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id;
