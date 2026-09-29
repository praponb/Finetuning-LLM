SELECT job_title, MAX(salary) AS max_salary FROM employees GROUP BY 1 ORDER BY job_title;
