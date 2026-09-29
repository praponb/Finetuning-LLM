SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.id, d.dept_name
