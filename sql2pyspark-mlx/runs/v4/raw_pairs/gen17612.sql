SELECT id, job_title, salary, MIN(salary) OVER (PARTITION BY job_title) AS job_title_min_salary FROM employees
