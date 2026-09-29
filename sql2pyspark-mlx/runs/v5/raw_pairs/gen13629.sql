SELECT id, is_remote, hire_date, CONCAT_WS(',', email, job_title) AS email_job_title_key FROM employees WHERE dept_id IN (6, 11, 12, 19) AND dept_id = 35 ORDER BY hire_date DESC, id ASC;
