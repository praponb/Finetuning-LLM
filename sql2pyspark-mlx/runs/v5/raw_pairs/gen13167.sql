SELECT d.id, d.budget, d.dept_name
FROM departments d
WHERE d.dept_name IN ('Sales', 'Legal', 'HR') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.job_title NOT IN ('Analyst', 'Manager', 'Designer'))
