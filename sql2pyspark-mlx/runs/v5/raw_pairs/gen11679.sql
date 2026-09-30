SELECT
    id,
    job_title,
    hire_date,
    manager_id,
    TRIM(email) AS clean_email
FROM employees
WHERE manager_id IS NOT NULL OR email NOT LIKE '%@outlook.com';
