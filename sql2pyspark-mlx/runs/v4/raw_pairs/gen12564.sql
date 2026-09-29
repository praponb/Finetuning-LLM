SELECT job_title, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE is_remote ORDER BY job_title DESC, is_remote_flag DESC;
