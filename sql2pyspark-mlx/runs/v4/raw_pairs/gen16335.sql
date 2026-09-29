SELECT d.id, d.dept_name FROM departments d WHERE d.dept_name IN ('Engineering', 'Marketing') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 69000);
