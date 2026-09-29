SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    m.hire_date AS manager_hire_date,
    m.is_remote AS manager_is_remote,
    m.job_title AS manager_job_title
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE m.is_remote AND e.manager_id IN (15, 21, 34)
ORDER BY manager_hire_date, e.manager_id DESC;
