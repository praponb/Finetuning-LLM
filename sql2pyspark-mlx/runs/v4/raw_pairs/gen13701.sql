SELECT *, LENGTH(job_title) AS job_title_length, CONCAT_WS('/', job_title, full_name) AS job_title_full_name_key FROM employees
