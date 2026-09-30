SELECT d.id, d.budget, d.dept_name, d.location FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.level NOT IN ('L1', 'L4'))
