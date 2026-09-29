SELECT d.id, d.dept_name FROM departments d WHERE d.dept_name = 'Engineering' AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date <= '2023-07-01')
