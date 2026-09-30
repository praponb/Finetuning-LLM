SELECT id, is_remote, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE job_title = 'Designer' OR job_title IN ('Engineer', 'Manager', 'Director');
