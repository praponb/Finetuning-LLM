SELECT id, job_title, salary, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS salary_row FROM employees WHERE dept_id IN (8, 28, 36) ORDER BY job_title;
