SELECT e.id, e.dept_id, e.is_remote, e.salary, d.dept_name, d.location, LOWER(d.dept_name) AS dept_name_lower FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.full_name IS NULL;
