SELECT e.id, e.full_name, e.manager_id, e.level, d.location FROM employees e LEFT JOIN departments d ON e.dept_id = d.id
