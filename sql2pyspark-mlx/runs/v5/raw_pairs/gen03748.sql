SELECT e.id, e.manager_id, e.is_remote, e.hire_date, d.location, d.dept_name, d.budget FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.job_title IN ('Analyst', 'Engineer')
