SELECT d.id, d.dept_name, d.location, d.budget FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id = 6) ORDER BY d.budget DESC
