SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.manager_id AS employee_manager_id,
    m.full_name AS manager_full_name,
    m.is_remote AS manager_is_remote
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.job_title = 'Analyst' OR m.is_remote = TRUE;
