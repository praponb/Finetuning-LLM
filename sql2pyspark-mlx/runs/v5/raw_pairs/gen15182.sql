SELECT id, dept_id, email, job_title FROM employees WHERE full_name LIKE '%FRANK%' AND MONTH(hire_date) IN (6, 7, 8);
