SELECT job_title, MAX(salary) - MIN(salary) AS salary_range, AVG(salary) AS avg_salary FROM employees WHERE LENGTH(email) >= 7 GROUP BY job_title ORDER BY job_title
