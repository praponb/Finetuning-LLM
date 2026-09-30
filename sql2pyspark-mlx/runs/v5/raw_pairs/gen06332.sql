SELECT job_title, YEAR(hire_date) AS hire_year, SUM(salary) AS total_salary FROM employees WHERE dept_id = 15 GROUP BY 1, 2 ORDER BY hire_year DESC
