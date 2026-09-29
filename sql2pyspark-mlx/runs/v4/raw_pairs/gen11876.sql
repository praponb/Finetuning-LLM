SELECT e.id, e.full_name, e.hire_date, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.id DESC
