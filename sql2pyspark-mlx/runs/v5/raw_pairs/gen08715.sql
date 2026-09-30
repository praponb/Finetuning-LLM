SELECT d.id, d.location, d.budget FROM departments d WHERE d.dept_name IN ('Legal', 'Marketing', 'Finance') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);
