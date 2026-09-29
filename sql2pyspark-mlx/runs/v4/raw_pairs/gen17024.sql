SELECT full_name, is_remote, COALESCE(email, 'Unknown') email_filled FROM employees WHERE job_title IN ('Designer', 'Director')
