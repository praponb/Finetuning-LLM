SELECT e.id, e.manager_id, d.budget
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
