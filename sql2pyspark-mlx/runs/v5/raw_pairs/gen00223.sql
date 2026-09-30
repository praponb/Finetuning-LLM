SELECT d.location, MAX(e.hire_date) last_hire_date, AVG(e.salary) avg_salary FROM employees e INNER JOIN departments d ON d.id = e.dept_id GROUP BY d.location;
