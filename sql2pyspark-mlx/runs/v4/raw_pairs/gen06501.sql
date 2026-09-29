SELECT e.id, e.job_title, d.location, d.dept_name FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id WHERE d.budget > 2840000;
