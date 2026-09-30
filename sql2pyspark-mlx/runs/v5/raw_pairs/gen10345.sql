SELECT d.dept_name, ROUND(AVG(e.salary), 2) AS avg_salary, MIN(e.salary) AS min_salary FROM employees e INNER JOIN departments d ON e.dept_id = d.id GROUP BY d.dept_name;
