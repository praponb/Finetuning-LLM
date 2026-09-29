SELECT d.id, d.location FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.email LIKE '%@outlook.com') ORDER BY d.id;
