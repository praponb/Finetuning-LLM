SELECT e.id, e.hire_date, d.dept_name, d.location, d.budget FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY d.location, e.id DESC
