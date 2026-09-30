SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE YEAR(hire_date) >= 2023) PIVOT (COUNT(salary) FOR job_title IN ('Manager', 'Director'))
