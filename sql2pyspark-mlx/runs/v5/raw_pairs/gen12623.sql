SELECT id, level, salary, MIN(salary) OVER (PARTITION BY level) AS level_min_salary FROM employees WHERE job_title = 'Engineer'
