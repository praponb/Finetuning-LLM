SELECT d.location, e.manager_id FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.job_title = 'Engineer';
