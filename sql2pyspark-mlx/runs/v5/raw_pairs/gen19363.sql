SELECT e.id, e.job_title, d.budget, d.dept_name, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.dept_id = 30;
