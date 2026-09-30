SELECT id, hire_date, email, job_title FROM employees WHERE manager_id IN (2, 6, 17) AND (level <> 'L4' OR email LIKE '%@company.co')
