SELECT id, job_title, level, CONCAT_WS('-', full_name, level) AS full_name_level_key, COALESCE(email, 'N/A') AS email_filled FROM employees ORDER BY level, id LIMIT 20
