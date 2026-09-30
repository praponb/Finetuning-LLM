SELECT
    e.id,
    e.level employee_level,
    e.full_name employee_full_name,
    m.job_title manager_job_title,
    m.salary manager_salary,
    m.is_remote manager_is_remote
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.dept_id = 29
ORDER BY id DESC;
