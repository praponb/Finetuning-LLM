SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.hire_date AS employee_hire_date,
    m.job_title AS manager_job_title,
    m.level AS manager_level,
    m.manager_id AS manager_manager_id
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IS NULL AND e.dept_id IN (3, 4, 9, 40);
