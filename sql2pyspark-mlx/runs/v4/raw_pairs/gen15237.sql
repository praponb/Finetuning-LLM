SELECT job_title, hire_date FROM employees WHERE LOWER(TRIM(email)) = 'bob.smith@yahoo.com' AND manager_id = 2;
