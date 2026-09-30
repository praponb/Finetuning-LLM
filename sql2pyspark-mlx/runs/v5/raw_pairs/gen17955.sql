SELECT id, job_title, hire_date, salary, LAG(salary, 2) OVER (PARTITION BY job_title ORDER BY hire_date, id) prev_salary FROM employees ORDER BY hire_date DESC
