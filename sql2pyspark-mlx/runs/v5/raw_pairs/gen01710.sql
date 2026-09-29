SELECT id, is_remote, level, LENGTH(email) AS email_length FROM employees WHERE full_name NOT LIKE '%Ford%' AND job_title = 'Designer';
