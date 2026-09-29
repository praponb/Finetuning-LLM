SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.job_title AS employee_job_title,
    e.hire_date AS employee_hire_date,
    m.hire_date AS manager_hire_date,
    m.manager_id AS manager_manager_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.full_name LIKE '%Emma%';
