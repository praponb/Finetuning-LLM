SELECT id, level, job_title, UPPER(email) AS email_upper FROM employees WHERE job_title = 'Intern' AND dept_id IN (12, 17, 28) ORDER BY 2
