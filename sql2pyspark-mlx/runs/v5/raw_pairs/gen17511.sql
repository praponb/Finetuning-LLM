SELECT id, job_title, full_name, CAST(salary AS INT) salary_int FROM employees WHERE level IS NOT NULL
