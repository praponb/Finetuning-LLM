SELECT *, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label, WEEKOFYEAR(hire_date) AS hire_date_weekofyear FROM employees WHERE job_title = 'Director' OR LENGTH(full_name) > 12
