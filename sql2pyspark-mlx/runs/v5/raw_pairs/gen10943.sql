SELECT
    e.id,
    e.job_title employee_job_title,
    e.email employee_email,
    e.salary employee_salary,
    m.is_remote manager_is_remote
FROM employees e
JOIN employees m ON e.manager_id = m.id
ORDER BY e.id;
