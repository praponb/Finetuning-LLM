SELECT id, job_title, level, email, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date FROM employees WHERE email LIKE '%@outlook.com' AND dept_id IN (3, 7);
