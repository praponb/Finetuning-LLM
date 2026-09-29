SELECT e.id, e.is_remote, e.full_name, d.dept_name, d.location FROM employees e INNER JOIN departments d ON d.id = e.dept_id WHERE e.job_title <> 'Manager'
