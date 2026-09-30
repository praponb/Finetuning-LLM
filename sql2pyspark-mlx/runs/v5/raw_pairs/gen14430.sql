SELECT d.dept_name, d.location, e.is_remote FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY d.dept_name
