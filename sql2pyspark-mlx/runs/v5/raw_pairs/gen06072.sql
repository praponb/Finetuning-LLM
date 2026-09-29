SELECT d.location, d.dept_name, d.budget, e.job_title, e.full_name, e.salary FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.dept_id IN (14, 19, 31) AND e.full_name LIKE '%Dav%'
