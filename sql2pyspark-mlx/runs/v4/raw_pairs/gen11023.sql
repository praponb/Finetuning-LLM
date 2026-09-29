SELECT e.full_name, e.hire_date, e.job_title, d.location FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id WHERE YEAR(e.hire_date) = 2023 ORDER BY location DESC;
