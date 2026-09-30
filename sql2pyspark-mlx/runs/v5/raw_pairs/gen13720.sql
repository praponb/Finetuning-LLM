SELECT e.id, e.manager_id, e.is_remote, d.location, d.budget, d.dept_name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id
