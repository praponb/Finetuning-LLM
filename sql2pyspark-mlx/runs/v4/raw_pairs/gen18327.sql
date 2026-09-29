SELECT id, level, manager_id, CAST(salary AS INT) AS salary_int, CONCAT_WS(',', email, job_title) AS email_job_title_key FROM employees
