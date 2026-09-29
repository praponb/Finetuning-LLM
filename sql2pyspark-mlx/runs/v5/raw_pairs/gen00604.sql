SELECT id, email, hire_date, job_title, CAST(is_remote AS INT) is_remote_flag FROM employees WHERE salary >= 72000 OR LENGTH(email) > 7 ORDER BY is_remote_flag DESC
