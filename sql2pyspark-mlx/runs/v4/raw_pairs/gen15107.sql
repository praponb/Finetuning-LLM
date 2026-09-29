SELECT
    e.id,
    e.salary AS employee_salary,
    m.is_remote AS manager_is_remote,
    m.dept_id AS manager_dept_id,
    m.job_title AS manager_job_title
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE m.email LIKE '%@startup.io' OR UPPER(TRIM(m.email)) = 'DAVID.LEE@GMAIL.COM'
ORDER BY manager_dept_id DESC, employee_salary DESC;
