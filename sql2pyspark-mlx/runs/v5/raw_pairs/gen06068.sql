SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.hire_date AS employee_hire_date,
    e.full_name AS employee_full_name,
    m.manager_id AS manager_manager_id,
    m.job_title AS manager_job_title,
    m.salary AS manager_salary
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.manager_id IS NOT NULL AND m.dept_id IN (15, 25, 29)
ORDER BY e.full_name ASC;
