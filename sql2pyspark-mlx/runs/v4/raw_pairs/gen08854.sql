SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    e.job_title AS employee_job_title,
    e.level AS employee_level,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY 1 ASC;
