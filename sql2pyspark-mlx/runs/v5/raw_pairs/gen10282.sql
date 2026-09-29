SELECT e.id, e.dept_id, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.job_title = 'Intern' ORDER BY 1 ASC;
