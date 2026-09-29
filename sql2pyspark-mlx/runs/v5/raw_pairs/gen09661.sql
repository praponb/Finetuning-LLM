SELECT d.budget, d.dept_name, e.job_title FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY 3;
