SELECT
    e.id,
    e.dept_id AS employee_dept_id,
    e.manager_id AS employee_manager_id,
    m.is_remote AS manager_is_remote
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE YEAR(e.hire_date) >= 2024 OR m.job_title <> 'Engineer'
ORDER BY id
LIMIT 5
