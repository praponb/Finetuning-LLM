SELECT id, job_title, salary, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary) AS dense_rnk FROM employees WHERE salary <= 78000;
