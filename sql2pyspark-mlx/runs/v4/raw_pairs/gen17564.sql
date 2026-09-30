SELECT MAX(hire_date) AS last_hire_date, SUM(salary) AS total_salary, ROUND(AVG(salary), 2) AS avg_salary FROM employees WHERE dept_id IN (1, 22, 27, 40)
