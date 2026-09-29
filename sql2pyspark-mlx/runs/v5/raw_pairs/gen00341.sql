SELECT
    id,
    is_remote,
    job_title,
    manager_id
FROM employees
WHERE email NOT LIKE '%@company.co'
ORDER BY manager_id DESC
