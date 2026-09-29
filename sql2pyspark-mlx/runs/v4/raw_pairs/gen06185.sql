SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.dept_id AS employee_dept_id,
    e.job_title AS employee_job_title,
    m.is_remote AS manager_is_remote
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.salary > 92000
ORDER BY employee_dept_id, id
LIMIT 5;
