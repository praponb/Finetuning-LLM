SELECT d.id, d.location, d.dept_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY id DESC, location DESC;
