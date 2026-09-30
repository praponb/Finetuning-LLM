SELECT e.id, e.manager_id, e.job_title, e.hire_date FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location = 'London')
