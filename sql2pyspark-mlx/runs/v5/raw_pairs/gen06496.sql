SELECT d.location, MIN(e.hire_date) AS first_hire_date, AVG(e.salary) AS avg_salary FROM employees e INNER JOIN departments d ON e.dept_id = d.id GROUP BY d.location ORDER BY location, avg_salary
