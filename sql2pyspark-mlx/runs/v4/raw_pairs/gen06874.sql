SELECT COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) AS analyst_count, MIN(hire_date) AS min_hire_date FROM employees
