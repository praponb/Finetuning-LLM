SELECT
    e.is_remote employee_is_remote,
    e.job_title employee_job_title,
    e.level employee_level,
    m.manager_id manager_manager_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.full_name IS NOT NULL
ORDER BY employee_job_title ASC;
