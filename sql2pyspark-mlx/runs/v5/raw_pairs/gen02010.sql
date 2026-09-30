SELECT
    e.job_title AS employee_job_title,
    e.email AS employee_email,
    e.hire_date AS employee_hire_date,
    m.level AS manager_level,
    m.full_name AS manager_full_name
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE m.dept_id = 13
