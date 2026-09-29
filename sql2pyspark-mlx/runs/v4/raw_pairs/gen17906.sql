SELECT id, job_title, hire_date, salary, LEAD(salary, 1) OVER (PARTITION BY job_title ORDER BY hire_date, id) AS next_salary FROM employees WHERE manager_id IN (10, 12, 14) ORDER BY id DESC;
