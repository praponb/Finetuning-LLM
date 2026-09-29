SELECT d.id, d.budget FROM departments d WHERE d.dept_name IS NULL AND d.id IN (SELECT dept_id FROM employees WHERE job_title != 'Manager')
