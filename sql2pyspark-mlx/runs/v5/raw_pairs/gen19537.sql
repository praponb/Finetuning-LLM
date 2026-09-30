SELECT d.dept_name, d.budget, d.location, e.job_title FROM departments d LEFT JOIN employees e ON e.dept_id = d.id;
