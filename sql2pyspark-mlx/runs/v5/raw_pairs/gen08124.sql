SELECT e.id, e.job_title, e.email, e.level, d.dept_name, d.location FROM employees e JOIN departments d ON d.id = e.dept_id
