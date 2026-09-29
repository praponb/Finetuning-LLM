SELECT d.id, d.dept_name FROM departments d WHERE d.budget >= 2635000 AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
