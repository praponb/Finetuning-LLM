SELECT level, manager_id, SUM(salary) AS total_salary, ROUND(AVG(salary), 2) AS avg_salary FROM employees GROUP BY level, manager_id
