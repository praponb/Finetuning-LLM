SELECT e.id, e.full_name, e.job_title, e.dept_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'HR') ORDER BY full_name;
