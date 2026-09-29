SELECT id, job_title, full_name, IF(is_remote, 'Yes', 'No') AS is_remote_label, DATE_SUB(hire_date, 63) AS hire_date_minus_63d FROM employees
