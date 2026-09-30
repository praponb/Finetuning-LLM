SELECT job_title FROM employees WHERE dept_id IN (9, 16, 39) UNION SELECT job_title FROM employees WHERE is_remote = FALSE;
